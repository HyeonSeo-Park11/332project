# 332project

## Week1 Progress Report

**Progress in the week:** initial setup

- create github repository
- add collaborators
- requests for access server cluster

**Goal of the next week:** Design entire software without code & Preparing the Basics for Collaboration

- Understanding the details of the project through discussion
- Make agreement for scala & sbt verison for collaborative work
- Decide the team leader
- Set specific milestones for each week (Specific milestones will be planned starting next week due to preparations for the midterm exams.)

**Goal of the next week for each individual member:** for design period, every work will processed by meeting

- For All : Read the project pptx file and comprehending what we have to make - Reviewed the project goal of distributed, fault-tolerant sorting and all requirements

## Week2 Progress Report

**Progress in the week**: Design Phase 1/2

- Decide coding convention
  - https://github.com/kmw14641/332project/blob/main/docs/coding_convention.md
- Establish overall design except designs for code structure
  - https://github.com/kmw14641/332project/blob/main/docs/design.md

**Goal of the next week:** Design Phase 2/2, Start of distribute Work

- Establish the structure of the code (class, directory...etc)
- Envionment setting(sbt, jdk, scala, initial repo setting)
- Setting milestones
- Allocate individual task
**Goal of the next week for each individual member**: for design period, every work will processed by meeting

## Week3 Progress Report

**Progress in the week** 

- Allocate individual task
  - MinWoo Kim : Shuffle phase
  - JongWon Lee : Disk-based merge sort phase
  - HyeonSeo Park : Environment setting
- Setting [milestones](https://github.com/kmw14641/332project/blob/main/docs/Milestone.md)
- Implement some part of software

**Goal of the next week:** Almost finish to implement software
  
**Goal of the next week for each individual member**
- HyeonSeo Park : Finalizing environmental setting both for server & local
- JongWon Lee : Finish implementing Disk-based merge sort
- MinWoo Kim : Finish implementing Shuffle phase

## Week4 Progress Report

**Progress in the week** 

- Development environment setting
  - set docker-compose for imitate server network environment @HyeonSeo Park
    - https://github.com/kmw14641/332project/pull/2 - Merged
    - Milestone 1-2 complete!
  - automate gensort execution @JongWon Lee
    - https://github.com/kmw14641/332project/pull/4 - Merged
  - update server environment @HyeonSeo Park
- Implement start part of program
  - set repository structure, design code frame @MinWoo Kim
    - https://github.com/kmw14641/332project/pull/1 - Merged
  - implement master, worker command line parser @JongWon Lee
    - https://github.com/kmw14641/332project/pull/5 - Reviewing
    - https://github.com/kmw14641/332project/pull/6 - Reviewing
- Implement sampling phase @JongWon Lee
  - https://github.com/kmw14641/332project/pull/7 - Reviewing
  - https://github.com/kmw14641/332project/pull/8 - Reviewing
- Implement shuffle phase @MinWoo Kim
  - https://github.com/kmw14641/332project/pull/9 - Reviewing
- The implementation goal for the week was not fully met
  - This could be due to the work being divided into more detailed phases than anticipated

**Goal of the next week:**
- Finish implementation of all part, including fault tolerance
- No need to precisely work, refactoring tasks can be remained
- Prepare presentation
- Write docs for individual parts
  
**Goal of the next week for each individual member**
- JongWon Lee : Finish implementing Disk-based merge sort
- HyeonSeo Park : Finish implementing Synchronization Phase, State Restoration
- MinWoo Kim : Finish implementing State Restoration

## Week5 Progress Report

**Progress in the week** 

- Complete Review and Merged
  - #5, #6, #7, #8
- Implement disk-based merge sort phase & labeling phase @JongWon Lee
  - https://github.com/kmw14641/332project/pull/20 - Reviewing
- Implment synchronization phase @HyeonSeo Park
  - https://github.com/kmw14641/332project/pull/21 - Reviewing
- Reimplement shuffle phase by server environment testing @MinWoo Kim
  - https://github.com/kmw14641/332project/pull/23 - Reviewing
- Fixes
  - Gensort was not generated randomly
    - https://github.com/kmw14641/332project/pull/19 - Reviewing
  - Docker Container did not work on Mac
    - https://github.com/kmw14641/332project/pull/22 - Reviewing
- Refactorings
  - Re-establish folder structure @MinWoo Kim
    - https://github.com/kmw14641/332project/pull/24 - Reviewing
- Making presentation materials
- Fault tolerance was not implemented, because of reimplementations


**Goal of the next week:**
- Finish implementation of all part
  - Review & merge of current PR
  - Implement state restoration (fault tolerance)
  - Code refactoring
  
**Goal of the next week for each individual member**
- For All : Possible code refactoring, check completeness of program
- JongWon Lee : Finish implementing Final Merge, State Restoration
- HyeonSeo Park : Finish implementing State Restoration
- MinWoo Kim : Finish implementing State Restoration

## Week6 Progress Report

**Progress in the week** 

- Previous goal was not met because, by professor’s comments on presentation, we decide to integrate our code first, check correctness of code, and then implement state restoration.
- However, there was some challenges: 
1. a lot of resource was needed for code reviewing
2. not that easy to merge code clearly, which seemed to independent but actually not that much
3. all team member got lack of time this week, bothered with other subject’s assignment, exam, paying the price of time spilled on week 5 and progress presentation preparation
- So, what we did:
  - make more progress on current reviewing pull requests
    - https://github.com/kmw14641/332project/pull/20
      - fix edge case that sampling is not completed when disk base merge sort is completed
      - supplement error handlings on disk base merge sort
    - https://github.com/kmw14641/332project/pull/23
      - fix problem that shuffle stream did not wait current chunk complete (only waited current file complete)
    - https://github.com/kmw14641/332project/pull/21
      - simplifying or removing unused logic
  - refactoring works which helps integration of code
    - add ConnectionManager, which is responsible for gRPC channel
      - https://github.com/kmw14641/332project/pull/26
    - divide merged logics to seperated Managers, which are responsible for same phase
      - https://github.com/kmw14641/332project/pull/27

**Goal of the next week:**
- Merge pull requests (not that trivial)
- Check correctness of distributed sorting without fault tolerance
- Implement state restoration
  
**Goal of the next week for each individual member**
- JongWon Lee : merge pull request, implement final merge, termination logic
- HyeonSeo Park : merge pull request, check correctness of program
- MinWoo Kim : merge pull request, implement state restoration

## Week7 Progress Report

**Progress in the week** 

- merged pull requests
  - disk based merge sort - https://github.com/kmw14641/332project/pull/20
  - synchronize - https://github.com/kmw14641/332project/pull/21
  - shuffle - https://github.com/kmw14641/332project/pull/23
- above pull requests include applying of refactoring
  - connection manager - https://github.com/kmw14641/332project/pull/26
  - increase readability of Main code using manager pattern - https://github.com/kmw14641/332project/pull/27
- implement termination phase - https://github.com/kmw14641/332project/pull/33 (merged)
- check correctness of program
  - in local environment and small data, all worker has sorted data in order!
  - some additional work is needed: not precisely met output format, temporary code removal or refactorings
- implement state restoration (in progress)
  - PoC of serialize/deserialize: https://github.com/kmw14641/332project/tree/feat/state-restoration

**Goal of the next week:**
- refactorings
- implement fault tolerance and check correctness
- final test on server environment
- more last works if possible
- prepare final presentation
  
**Goal of the next week for each individual member**
- JongWon Lee : fault tolerance on disk based merge sort
- HyeonSeo Park : fault tolerance on sync phase
- MinWoo Kim : implement state restoration infra, fault tolerance on shuffle phase