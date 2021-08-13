use core::panic;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::collections::VecDeque;
use num_cpus;
use smallbox::*;
pub enum SpawnChildJobs {
    /// No child jobs are given
    None,
    /// enqueues jobs immediately as part of the parent batch
    AsPartOfBatch(Vec<JobTraitBox>),
    /// enqueues orphan child jobs after the completion of the batch of the parent job
    AfterBatch(Vec<JobTraitBox>),
    /// enqueues orphan child jobs immediately after completion of the parent job
    AfterCurrentJob(Vec<JobTraitBox>),
}

pub trait Job: Send + Sync {
    fn run(&mut self, worker_id: usize) -> SpawnChildJobs;
}

struct JobBatchInfoSlot {
    batch: Option<JobBatchInfo>,
    generation: u64,
}

struct JobBatchInfo {
    tracked: bool,
    left: u32,
    following_jobs: Vec<JobQueueEntry>,
}

impl JobBatchInfo{
    fn new(left: u32) -> Self {
        Self {
            tracked: true,
            left: left,
            following_jobs: Vec::new(),
        }
    }
}

pub type JobTraitBox = SmallBox<dyn Job, space::S32>;

pub struct JobBatchId {
    index: usize,
    generation: u64,
}

struct JobQueueEntry {
    job: JobTraitBox,
    job_batch_index: Option<usize>,
}

struct JobContext {
    queue: VecDeque<JobQueueEntry>,
    workers_die: bool,
    job_batches: Vec<JobBatchInfoSlot>,
    job_batches_free_list: Vec<usize>,
}

impl JobContext {
    fn new() -> Self {
        Self {
            queue: VecDeque::new(),
            workers_die: false,
            job_batches: Vec::new(),
            job_batches_free_list: Vec::new(),
        }
    }

    fn delete_batch(&mut self, index: usize) {
        self.job_batches[index].generation += 1;
        self.job_batches[index].batch = None;
        self.job_batches_free_list.push(index);
    }
}

struct MetaContext {
    context: Mutex<JobContext>,
    worker_cv: Condvar,
    client_cv: Condvar,
}

impl MetaContext {
    fn new() -> Self {
        Self{
            context: Mutex::new(JobContext::new()),
            worker_cv: Condvar::new(),
            client_cv: Condvar::new(),
        }
    }
}

// TODO(refactor into smaller functions)
fn worker_function(meta_context: Arc<MetaContext>, worker_id: usize) {
    let context = &meta_context.context;
    let worker_cv = &meta_context.worker_cv;
    let client_cv = &meta_context.client_cv;

    let mut guard = context.lock().unwrap();
    loop {
        let mut pot_job = None;

        guard = worker_cv
            .wait_while(guard, |g| { 
                match g.queue.pop_front() {
                    Some(j) => {pot_job = Some(j); false},
                    None => {
                        if g.workers_die {
                            false
                        } else {
                            true
                        }
                    }
                }
             })
            .unwrap();

        let (mut job, job_batch_index) = 
            match pot_job {
                None => break,
                Some(entry) => {
                    (entry.job, entry.job_batch_index)
                }
            };
            
        std::mem::drop(guard);
        let mut child_jobs = job.run(worker_id);
        guard = context.lock().unwrap();

        match &mut child_jobs {
            SpawnChildJobs::AfterCurrentJob(child_jobs) => {
                while let Some(job) = child_jobs.pop() {
                    guard.queue.push_back(JobQueueEntry{job:job, job_batch_index:None});
                }
            },
            _ => {}
        }

        match job_batch_index{
            Some(job_batch_index) => {
                let mut jobs_to_immediately_enqueue = Vec::new();
                let mut delete_batch = false;
    
                match guard.job_batches[job_batch_index].batch {
                    Some(ref mut batch) => {

                        // if we have a job in a batch and spawn children after completion:
                        match &mut child_jobs {
                            SpawnChildJobs::AfterBatch(child_jobs) => {
                                while let Some(job) = child_jobs.pop() {
                                    batch.following_jobs.push(JobQueueEntry{job:job, job_batch_index:None});
                                }
                            },
                            SpawnChildJobs::AsPartOfBatch(child_jobs) => {
                                while let Some(job) = child_jobs.pop() {
                                    batch.left += 1;
                                    jobs_to_immediately_enqueue.push(JobQueueEntry{job:job, job_batch_index:Some(job_batch_index)});
                                }
                            },
                            _ => {}
                        }

                        // the job was completed, therefore we reduce the jobs left count of the batch
                        batch.left -= 1;
    
                        // if there are no jobs left in the batch, the batch is completed
                        if batch.left == 0 {
                            // save all following jobs to be enqueued immediately after completion
                            while let Some(job) = batch.following_jobs.pop() {
                                jobs_to_immediately_enqueue.push(job);
                            }

                            if batch.tracked {
                                client_cv.notify_all();
                            } else {
                                delete_batch = true;
                            }
                        }
                    },
                    None => panic!("job tried to index its job batch, but it was not there! index was: {}. Maybe the index is wrong or the job batch was destroyed too soon.", job_batch_index)
                }

                if jobs_to_immediately_enqueue.len() > 0 {
                    worker_cv.notify_all();
                }
                
                while let Some(job) = jobs_to_immediately_enqueue.pop() { 
                    guard.queue.push_back(job);
                }
        
                if delete_batch {
                    guard.delete_batch(job_batch_index);
                }
            },
            None => {
                // if our job is not part of a batch we start enqueue the jobs as untracked without job batches
                match child_jobs {
                    SpawnChildJobs::AfterBatch(_dummy) => {
                        panic!("can not spawn child jobs to start after batch, because parent job has no batch!");
                    },
                    SpawnChildJobs::AsPartOfBatch(_dummy) => {
                        panic!("can not spawn child jobs to start as part of batch, because parent job has no batch!");
                    },
                    _ => {}
                }
            }
        }
    }
}

pub enum Dispatch {
    Immediate,
    AfterBatch(JobBatchId),
}

pub struct JobManager {
    workers: Vec<std::thread::JoinHandle<()>>,
    shared_context: Arc<MetaContext>,
}

impl JobManager {
    pub fn new() -> Self {
        let meta_context = Arc::new(MetaContext::new());

        let worker_count = num_cpus::get_physical() - 1;
        let mut workers = Vec::new();

        for worker_id in 0..worker_count {
            let context = meta_context.clone();
            let id = worker_id.clone();
            workers.push(std::thread::Builder::new().name("job system worker ".to_string() + &id.to_string()).spawn(move || {worker_function(context, id)}).unwrap());
        }

        Self {
            workers: workers,
            shared_context: meta_context,
        }
    }

    pub fn enqueue_batch(&self, jobs: &mut Vec<JobTraitBox>, dispatch: Dispatch) -> JobBatchId {
        let mut g = self.shared_context.context.lock().unwrap();
        let (job_batch_index, job_batch_slot_generation) = self.get_new_job_batch(&mut g, jobs.len());

        match dispatch {
            Dispatch::AfterBatch(other_id) => {
                self.assure_id_is_valid(&g, &other_id);
                g.job_batches[other_id.index].batch.as_mut().unwrap().following_jobs = {
                    let mut following_jobs = Vec::<JobQueueEntry>::new();
                    while let Some(job) = jobs.pop() {
                        following_jobs.push(JobQueueEntry{job:job, job_batch_index: Some(job_batch_index)});
                    }
                    following_jobs
                };
            }
            Dispatch::Immediate => {
                while let Some(job) = jobs.pop() {
                    g.queue.push_back(JobQueueEntry{job:job, job_batch_index: Some(job_batch_index)});
                }
            }
        }

        self.shared_context.worker_cv.notify_all();

        JobBatchId{index: job_batch_index, generation: job_batch_slot_generation}
    }

    pub fn enqueue_single(&self, job: JobTraitBox, dispatch: Dispatch) -> JobBatchId {
        let mut g = self.shared_context.context.lock().unwrap();
        let (job_batch_index, job_batch_slot_generation) = self.get_new_job_batch(&mut g, 1);

        match dispatch {
            Dispatch::AfterBatch(other_id) => {
                self.assure_id_is_valid(&g, &other_id);
                g.job_batches[other_id.index].batch.as_mut().unwrap().following_jobs = vec![JobQueueEntry{job:job, job_batch_index: Some(job_batch_index)}];
            }
            Dispatch::Immediate => {
                g.queue.push_back(JobQueueEntry{job:job, job_batch_index: Some(job_batch_index)});
            }
        }

        self.shared_context.worker_cv.notify_all();

        JobBatchId{index: job_batch_index, generation: job_batch_slot_generation}
    }

    pub fn untrack(&self, job_id: JobBatchId) {
        let mut g = self.shared_context.context.lock().unwrap();
        self.assure_id_is_valid(&g, &job_id);
        g.job_batches[job_id.index].batch.as_mut().unwrap().tracked = false;
    }

    pub fn wait_for(&self, job_id: JobBatchId) {
        let mut g = self.shared_context.context.lock().unwrap();
        self.assure_id_is_valid(&g, &job_id);

        match g.job_batches[job_id.index].batch {
            None => {},
            Some(ref batch) => if !batch.tracked { panic!("Tried waiting on untracked job! index was: {}, generation was: {}",job_id.index, job_id.generation); }
        }
        
        g = self.shared_context.client_cv
            .wait_while(g, |g| { 
                match &g.job_batches[job_id.index].batch {
                    Some(batch) => { batch.left > 0 },
                    None => panic!("While waiting on a job batch to finish, the batch was deleted! This should not be possible when the job batch is tracked!")
                }
            })
            .unwrap();

        g.delete_batch(job_id.index);
    }

    pub fn finished(&self, job_id: &JobBatchId) -> bool {
        let mut g = self.shared_context.context.lock().unwrap();
        self.assure_id_is_valid(&g, &job_id);
        
        let batch = g.job_batches[job_id.index].batch.as_mut().unwrap();
        
        if !batch.tracked { panic!("Tried checking if finished on untracked job! index was: {}, generation was: {}",job_id.index, job_id.generation); }

        match batch.left {
            0 => {
                g.delete_batch(job_id.index);
                true
            }
            _ => false
        }
    }

    fn assure_id_is_valid(&self, g: &std::sync::MutexGuard<JobContext>, job_id: &JobBatchId) {
        match g.job_batches.get(job_id.index) {
            Some(_dummy) => {
                match g.job_batches[job_id.index].batch {
                    Some(ref _dummy) => {},
                    None => panic!("invalid job id! dont use job ids of untracked jobs!")
                };
            },
            None => panic!("Invalid job id! index is larger than job slot capacity! index was: {}.", job_id.index)
        }
    }

    fn get_new_job_batch(&self, guard: &mut MutexGuard<JobContext>, job_count: usize) -> (usize,u64) {
        let new_job_batch = JobBatchInfo::new(job_count as u32);

        if guard.job_batches_free_list.is_empty() {
            guard.job_batches.push(JobBatchInfoSlot{generation: 0, batch: Some(new_job_batch)});
            (guard.job_batches.len() - 1, guard.job_batches.last().unwrap().generation)
        } else {
            let index = guard.job_batches_free_list.pop().unwrap();
            guard.job_batches[index].batch = Some(new_job_batch);
            (index, guard.job_batches[index].generation)
        }
    }
}

impl Drop for JobManager {
    fn drop(&mut self) {
        self.shared_context.context.lock().unwrap().workers_die = true;
        self.shared_context.worker_cv.notify_all();

        while let Some(jh) = self.workers.pop() {
            jh.join().unwrap()
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
