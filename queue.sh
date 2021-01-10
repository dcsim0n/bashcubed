#/usr/bin/env bash

#create temp files and work queue
#can you lock fifo queue?

WORKERS=2

WORKER_NUMS=$(mktemp /tmp/start-XXXX)
QUEUE_FIFO=$(mktemp /tmp/workq-XXXX)
EXIT_CODES=$(mktemp /tmp/exitq-XXXX)
TOTAL_JOBS=0

trap queue_cleanup EXIT

#convert temp file to fifo
rm $QUEUE_FIFO
mkfifo $QUEUE_FIFO
echo "Queue File: $QUEUE_FIFO"
#define some jobs to do

somework(){
  sleep 5
  echo "do something with your life"
}

somefailure(){
  sleep 10
  exit 1
}

#define worker behavior
queue_worker(){
  #sub shell, different file descriptors
  #-open fifo queue for reading to fd #3
  #-open exit queue for writing exit status, fd #4
  #-link worker num file for locking
  exec 3<$QUEUE_FIFO
  exec 4<$EXIT_CODES
  exec 5<$WORKER_NUMS

  WORKER_NUM=$1

  flock 5
  echo $WORKER_NUM>>$WORKER_NUMS
  flock -u 5
#--will kick of worker spawn process
#-spawn a job in subshell
  while true
  do
    flock 3
    read -su 3 job_id job
    DID_READ_DATA=$?
    flock -u 3
    if [[ $DID_READ_DATA -eq 0 ]]
    then
      echo "Starting job: $job_id"
      ( "$job" )
#-capture exit status from subshell
#-write exit status to exit fifo
      JOB_EXIT_STAT=$?
      echo "Job id: $job_id finished with $JOB_EXIT_STAT"
      flock 4
      echo $job_id $JOB_EXIT_STAT >> $EXIT_CODES
      flock -u 4
    else
      break
    fi
  done
    echo "Worker $WORKER_NUM finished all work, cleaning up and exiting.."
  exec 3<&-
  exec 4<&-
  exec 5<&-
}

queue_cleanup(){
  rm $WORKER_NUMS
  rm $QUEUE_FIFO
  rm $EXIT_CODES
}

queue_start()
{
  echo "Queue start called. Starting $WORKERS worker threads"
  #initialize workers
  for(( i=1;i<=$WORKERS;i++ ))
  do
    echo "trying to start worker $i"
    queue_worker $i& #workers will wait for a job
  done
  #open fifo for writing to fd #3, will unblock workers
  exec 3>$QUEUE_FIFO
  exec 4<$WORKER_NUMS
  exec 5<$EXIT_CODES

  while true #start all the works, wait for them all to be ready
  do
    WORKERS_ACTIVE=$(wc -l $WORKER_NUMS | cut -d \  -f1)
    if [[ $WORKERS_ACTIVE -le 4 ]]
    then
      break # all workers have started, get on with it
    else
      echo "Waiting for workers to start.."
    fi
  done
  flock 4
  >$WORKER_NUMS #workers started, empty workers file for next use
  flock -u 4
  echo "All workers started. Queue is waiting for work."
}

queue_enqueue()
{
 job_id=$TOTAL_JOBS
 job_task="$1"
 let "TOTAL_JOBS++"
 echo "$job_id $job_task" >> $QUEUE_FIFO
}

queue_close(){
  echo "Closing queue. Workers will finish current job and exit."
  exec 3>&-
}

queue_check_success(){
  flock 4
  FAILS=$(grep -Ec '^[0-9]+ [^0]$' $EXIT_CODES)
  flock -u 4
  if [[ $FAILS -gt 0 ]]
  then
    echo "$FAILS workers failed"
    exit 1 
  #handle statuses
  fi
}

queue_start

#send jobs to the workers
queue_enqueue somework 
queue_enqueue somework
queue_enqueue somework
queue_enqueue somefailure
queue_enqueue somefailure

queue_close

wait

echo "done waiting"
#
queue_start
#
#send jobs to the workers
queue_enqueue somework 
queue_enqueue somefailure
queue_enqueue somefailure
#
queue_close
#
wait
## check exit status
queue_check_success