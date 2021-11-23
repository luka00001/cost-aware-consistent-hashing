# cost-aware-consistent-hashing
Implementation of consistent hashing with bounded loads, and Experimental changes to make it cost aware

## Code Structure
### Datasets
Each dataset is modeled as an array of tasks, where each task has an ID and a Cost. The ID can be used to map
the task to the appropriate "machine" in the "cluster" (thread in the threadpool). The cost is how long the thread should sleep for. We should be able to translate real world datasets into this form with assumptions as well
### DataGenerators
DataGenerator.java contains a bunch of functions for gernetating different data sets based on real world data and random samples from different distributions
### Controller
Controller.java should be responsible for perodically pulling a new batch of objects of the task queue, and sending them to the appropriate thread based on the Task ID (in the base implementation). The controller will also be programmed with some adjustments to better handle varrying loads.
### Workers
Workers should follow a simple, read from my task queue, sleep for the cost on the task, cycle. 
### ServerDecider
This is where the various algorithms are that decide which server to map a task to given the current state of the systems. Types are listed in AlgorithmType
### App
App.java should be responsible for running the experiments, gathering metrics and summarizing the data
