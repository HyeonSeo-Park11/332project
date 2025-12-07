# Distributed Sorting System

This project is a distributed sorting system implemented in Scala. It consists of a Master node that coordinates the sorting process and multiple Worker nodes that perform sampling, labeling, shuffling, and merging of data.

- Team Cyan
- Members : Minwoo Kim, Jongwon Lee, Hyeonseo Park
- Weekly Progress: https://github.com/kmw14641/332project/blob/main/docs/weekly_progress.md

## 0. System environment

- java17, scala 2.13, sbt 1.11.7 are installed on master and each workers
- pssh, pscp are installed on master.


## 1. Server Connection

master connection : 
```bash
ssh -p 7777 cyan@141.223.16.227
```

worker connection : should be executed inside the master node
```bash
ssh -p 22 cyan@2.2.2.N
```
The range of N is 101~120.


## 2. Compilation
### On master

Build the project to generate the executable JAR files for both Master and Worker.

```bash
# clone repository if repository is not cloned
git clone https://github.com/kmw14641/332project.git

# move inside the git repo
cd 332project

# Build Master & Worker
sbt master/assembly && sbt worker/assembly
```
*Note: master's output JAR is located at `master/target/scala-2.13/master.jar` and worker's output JAR is located at `worker/target/scala-2.13/worker.jar` (path may vary based on sbt configuration).*

---


## 3. Deployment

Deploy the compiled Worker JAR file to all worker nodes using `pscp`, which copy files to multiple servers in parallel

1. Prepare the host list in a text file. (ex: `~/hosts/hosts3.txt`)
2. Text file stores list of worker node's address  
   example of host file: 
   ```bash
   cyan@2.2.2.101
   cyan@2.2.2.102
   cyan@2.2.2.103
    ```
3. Sends the worker's jar file to the addresses specified in the text file.
    ```bash
    # Example: Deploying to /home/cyan
    # should be executed in 332project diretory
    pscp -h ~/hosts/hosts3.txt worker/target/scala-2.13/worker.jar /home/cyan
    ```

---

## 4. Execution

### Step 1: Start Master
Run the Master program on the master node.
```bash
cd 332project
java -jar master/target/scala-2.13/master.jar [num_workers]
```


### Step 2: Start Workers in parallel
Run all worker nodes using PSSH.
```bash
# on the master machine
pssh -h [host file location] -i "bash -l -c 'java -jar worker.jar [master IP:port] -I [input directory 1]	[input directory 2] … [input directory N] -O [output directory]'"
```
- This command runs worker.jar on all remote hosts listed in host file at the same time. 
- 'master IP: port' must be the one printed when the master machine is run. 
- Input directories are located under the /dataset directory. 
- Output directory should be located under the home directory. 

- **example usage: pssh -h ~/hosts/hosts3.txt -i "bash -l -c 'java -jar ~/worker.jar 10.1.25.21:36337 -I /dataset/small -O ~/output'"**

#### Alternative: Start Workers directly in the worker node
```bash
# on the worker machine home directory
java -jar worker.jar [master IP:port] -I [input directory 1] [input directory 2] … [input directory N] -O [output directory]
```

---

## 5. Monitoring & Troubleshooting

### View Logs
All output from each worker is captured into ~/log/worker.log automatically.
To check the logs of a specific worker in real-time:
```bash
ssh [worker-ip]
tail -f ~/log/worker.log
```

### Kill Existing Processes
If you want to force-kill the speific worker,
```bash
ssh cyan@[workerIp] "ps -ef | grep '[w]orker.jar' | awk '{print \$2}' | xargs -r kill -9"
```
or you can manually kill the worker by pressing Ctrl+C on each worker machine.
