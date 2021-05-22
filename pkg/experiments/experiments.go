// Copyright (c) 2019, Sylabs Inc. All rights reserved.
// Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
// This software is licensed under a 3-clause BSD license. Please consult the
// LICENSE.md file distributed with the sources of this project regarding your
// rights to use or distribute this software.

package experiments

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"time"

	"github.com/gvallee/go_exec/pkg/advexec"
	"github.com/gvallee/go_exec/pkg/results"
	"github.com/gvallee/go_hpc_jobmgr/pkg/implem"
	"github.com/gvallee/go_hpc_jobmgr/pkg/jm"
	"github.com/gvallee/go_hpc_jobmgr/pkg/job"
	"github.com/gvallee/go_hpc_jobmgr/pkg/launcher"
	"github.com/gvallee/go_hpc_jobmgr/pkg/mpi"
	"github.com/gvallee/go_software_build/pkg/app"
	"github.com/gvallee/go_software_build/pkg/buildenv"
	"github.com/gvallee/go_software_build/pkg/builder"
	"github.com/gvallee/validation_tool/pkg/platform"
)

type MPIConfig struct {
	// MPI holds all the details about the MPI implementation to use
	MPI *implem.Info

	// BuildEnv is the environment to use for the experiment(s)
	BuildEnv buildenv.Info
}

type ExperimentResult struct {
	Res     results.Result
	ExecRes advexec.Result
}

type Experiment struct {
	// id is an internal id assigned to the experiment
	id int

	// App gathers all the data about the application to include in the container
	App *app.Info

	// Result gathers all the data related to the result of an experiment
	Result *ExperimentResult

	// MpirunArgs is a list of mpirun arguments to use for the experiment
	MpirunArgs []string

	// Env gathers all the data related to how software has been installed
	Env *buildenv.Info

	// Platform gathers all the data required to execute experiments on a target platform
	Platform *platform.Info

	// LaunchScript is the path to a script used to launch the experiment (optional)
	LaunchScript string

	// RequiredModules is the list of modules to load before running the experiment
	RequiredModules []string

	// RunDir is the directory from which the experiment needs to be executed
	RunDir string

	MPICfg *MPIConfig

	// job is the job associated to the experiment (ATM the moment, only one at a time)
	job *job.Job

	// jobmgr used for the execution of the experiment
	jobMgr *jm.JM

	runtime *Runtime
}

type Experiments struct {
	// App gathers all the data about the application to include in the container
	App *app.Info

	MPICfg *MPIConfig

	List []*Experiment

	// Result gathers all the data related to the result of an experiment
	Result results.Result

	// Env gathers all the data related to how software has been installed
	Env *buildenv.Info

	// Platform gathers all the data required to execute experiments on a target platform
	Platform *platform.Info

	// RequiredModules is the list of modules to load before running the experiment
	RequiredModules []string

	// RunDir is the directory from which the experiments needs to be executed
	RunDir string
}

type Runtime struct {
	count int

	wg *sync.WaitGroup

	pendingExperiments []*Experiment

	runningExperiments []*Experiment

	maxRunningJobs int

	Started bool
}

/*
func postExecutionDataMgt(sysCfg *sys.Config, output string) (string, error) {
	if sysCfg.NetPipe {
		lines := strings.Split(output, "\n")
		for _, line := range lines {
			if strings.Contains(line, "Completed with") {
				tokens := strings.Split(line, " ")
				note := "max bandwidth: " + util.CleanupString(tokens[13]) + " " + util.CleanupString(tokens[14]) + "; latency: " + util.CleanupString(tokens[20]) + " " + util.CleanupString(tokens[21])
				return note, nil
			}
		}
	}
	return "", nil
}
*/

func newRuntime() *Runtime {
	r := new(Runtime)
	r.wg = new(sync.WaitGroup)
	r.wg.Add(1)
	r.count = 0
	r.maxRunningJobs = 1
	return r
}

func processOutput(execRes *advexec.Result, expRes *results.Result, appInfo *app.Info) error {
	/*
		var err error

		expRes.Note, err = postExecutionDataMgt(sysCfg, execRes.Stdout)
		if err != nil {
			return fmt.Errorf("failed process data post execution: %s", err)
		}

		if appInfo.ExpectedNote != "" {
			if !strings.Contains(expRes.Note, appInfo.ExpectedNote) {
				return fmt.Errorf("the data from the application's output does not match with the expected output: %s vs. %s", expRes.Note, appInfo.ExpectedNote)
			}
		}

		log.Println("NOTE: ", expRes.Note)
	*/

	return nil
}

func (r *Runtime) triggerExperiment() error {
	var expMPICfg mpi.Config
	var err error

	if len(r.pendingExperiments) == 0 {
		return nil
	}

	e := r.pendingExperiments[0]
	e.Result = new(ExperimentResult)

	fmt.Printf("Triggering experiment %d\n", e.id)
	b := new(builder.Builder)

	if e.job == nil {
		e.job = new(job.Job)
	}

	sysCfg, jobMgr, err := launcher.Load()
	if err != nil {
		e.Result.ExecRes.Err = fmt.Errorf("unable to load a launcher: %s", err)
		e.Result.Res.Pass = false
		log.Printf("unable to load a launcher: %s", err)
		goto ExpCompleted
	}
	e.jobMgr = &jobMgr

	if e.Env != nil {
		b.Env.ScratchDir = e.Env.ScratchDir
		b.Env.InstallDir = e.Env.InstallDir
		b.Env.BuildDir = e.Env.BuildDir
		b.App.Name = e.App.Name
		b.App.URL = e.App.URL

		err := b.Load(true)
		if err != nil {
			e.Result.ExecRes.Err = fmt.Errorf("unable to load a builder: %s", err)
			e.Result.Res.Pass = false
			log.Printf("unable to load a builder: %s", err)
			goto ExpCompleted
		}
		res := b.Install()
		if res.Err != nil {
			e.Result.ExecRes.Err = fmt.Errorf("unable to install the experiment software: %s", res.Err)
			e.Result.Res.Pass = false
			log.Printf("unable to install the experiment software: %s", res.Err)
			goto ExpCompleted
		}
	} else {
		log.Printf("no build environment defined, not trying to build application")
	}

	if e.RunDir != "" {
		sysCfg.ScratchDir = e.RunDir
	} else {
		if e.Env != nil {
			sysCfg.ScratchDir = e.Env.ScratchDir
		} else {
			// It is possible that an environment has not been specified
			// for example if a very basic application needs to be executed,
			// without the need to install it. In that context, we create a
			// temporary directory for the experiment
			sysCfg.ScratchDir, err = ioutil.TempDir("", "exp_")
			if err != nil {
				e.Result.ExecRes.Err = fmt.Errorf("unable to create temporary directory for experiment: %s", err)
				e.Result.Res.Pass = false
				log.Printf("unable to create temporary directory for experiment: %s", err)
				goto ExpCompleted
			}
			defer os.RemoveAll(sysCfg.ScratchDir)
		}
	}
	if e.RunDir == "" {
		e.RunDir = sysCfg.ScratchDir
	}

	if e.MPICfg != nil {
		expMPICfg.Implem.InstallDir = e.MPICfg.BuildEnv.InstallDir
		expMPICfg.UserMpirunArgs = e.MpirunArgs // fixme: add the default args we get from config file
		err = expMPICfg.Implem.Load()
		if err != nil {
			e.Result.ExecRes.Err = fmt.Errorf("unable to detect information about the MPI implementation to use: %s", err)
			e.Result.Res.Pass = false
			log.Printf("unable to detect information about the MPI implementation to use: %s", err)
			goto ExpCompleted
		}
	}

	if e.Platform != nil {
		e.job.Partition = e.Platform.Name
	}
	if e.App != nil {
		e.job.App.Name = e.App.BinName
		e.job.App.BinArgs = e.App.BinArgs
		e.job.App.BinName = e.App.BinName
		e.job.App.BinPath = e.App.BinPath
		e.job.Name = e.App.Name
	}
	e.job.RunDir = e.RunDir
	if e.LaunchScript != "" {
		e.job.BatchScript = e.LaunchScript
	}
	if len(e.RequiredModules) > 0 {
		e.job.RequiredModules = e.RequiredModules
	}

	fmt.Printf("Launching experiment %d\n", e.id)
	e.Result.Res, e.Result.ExecRes = launcher.Run(e.job, &expMPICfg, e.jobMgr, &sysCfg, nil)
	/*
		if !expRes.Pass {
			return false, expRes, execRes
		}
	*/
	if e.Result.ExecRes.Err != nil {
		e.Result.ExecRes.Err = fmt.Errorf("failed to run experiment: %s", e.Result.ExecRes.Err)
		/*
			err = launcher.SaveErrorDetails(&exp.HostMPI, &myContainerMPICfg.Implem, sysCfg, &execRes)
			if err != nil {
				e.Result.ExecRes.Err = fmt.Errorf("failed to save error details: %s", err)
			}
		*/
		e.Result.Res.Pass = false
		goto ExpCompleted
	}
	log.Printf("* Successful run - Analysing data...")

	err = processOutput(&e.Result.ExecRes, &e.Result.Res, e.App)
	if err != nil {
		e.Result.ExecRes.Err = fmt.Errorf("failed to process output: %s", err)
		e.Result.Res.Pass = false
		log.Printf("failed to process output: %s", err)
		goto ExpCompleted
	}

	log.Println("-> Experiment successfully executed")
	log.Printf("* Experiment's note: %s", e.Result.Res.Note)
	log.Printf("* Experiment's output: %s", e.Result.ExecRes.Stdout)

	e.Result.Res.Pass = true

ExpCompleted:
	// dequeue experiments
	r.pendingExperiments = r.pendingExperiments[1:]
	r.runningExperiments = append(r.runningExperiments, e)

	return e.Result.ExecRes.Err
}

func (e *Experiment) getStatus() jm.JobStatus {
	if e.jobMgr == nil {
		// The experiment is defined but not yet submitted
		return jm.StatusPending
	}
	status, err := e.jobMgr.JobStatus([]int{e.job.ID})
	if err != nil {
		return jm.StatusUnknown
	}
	if len(status) != 1 {
		return jm.StatusUnknown
	}
	return status[0]
}

func (r *Runtime) checkCompletions() {
	for idx, e := range r.runningExperiments {
		s := e.getStatus()
		if s == jm.StatusDone || s == jm.StatusStop {
			r.runningExperiments = append(r.runningExperiments[:idx], r.runningExperiments[idx+1:]...)
		}
	}
}

func (r *Runtime) serveJobQueue() error {
	var err error

	if len(r.pendingExperiments) > 0 {
		if len(r.runningExperiments) < r.maxRunningJobs || r.maxRunningJobs == 0 {
			err = r.triggerExperiment()
			if err != nil {
				fmt.Printf("Triggering event failed: %s", err)
				os.Exit(69)
				return err
			}
		}
	}

	if len(r.runningExperiments) > 0 {
		// Check for completion
		r.checkCompletions()
	}
	return nil
}

func (e *Experiment) Run(r *Runtime) error {
	e.id = -1
	if e.App == nil {
		return fmt.Errorf("application is undefined")
	}

	r.pendingExperiments = append(r.pendingExperiments, e)
	if !r.Started {
		r.Start()
	}
	e.runtime = r

	e.id = r.count
	r.count++

	return nil
}

func (e *Experiments) Run(r *Runtime) bool {
	validationStatus := true
	for _, exp := range e.List {
		if e.App != nil && exp.App == nil {
			exp.App = e.App
		}
		if e.Env != nil && exp.Env == nil {
			exp.Env = e.Env
		}
		if e.Platform != nil && exp.Platform == nil {
			fmt.Println("DBG: copying platform info")
			exp.Platform = e.Platform
		}
		if len(e.RequiredModules) > 0 {
			exp.RequiredModules = e.RequiredModules
		}
		if e.MPICfg != nil {
			exp.MPICfg = e.MPICfg
		}
		err := exp.Run(r)
		if err != nil && validationStatus {
			validationStatus = false
		}
	}
	return validationStatus
}

func (r *Runtime) Start() {
	if !r.Started {
		// Start the go routine that handles the job queue
		go func(r *Runtime) {
			// fixme: do this only if the runtime routine is not created yet
			for len(r.pendingExperiments) > 0 || len(r.runningExperiments) > 0 {
				err := r.serveJobQueue()
				if err != nil {
					fmt.Printf("unable to run experiments: %s", err)
					break
				}
				if len(r.pendingExperiments) > 0 {
					// Some jobs could be submitted right away, so we wait 10 minutes
					time.Sleep(10 * time.Minute)
				}
			}
			defer r.wg.Done()
			r.Started = false
		}(r)
	}
	r.Started = true
}

// Wait makes the current process wait for the termination of the webUI
func (r *Runtime) Wait() {
	r.wg.Wait()
}

func (e *Experiment) Wait() {
	if e.runtime == nil {
		fmt.Println("undefined runtime")
		return
	}

	for e.Result == nil {
		time.Sleep(1 * time.Second)
		s := e.getStatus()
		if s == jm.StatusDone || s == jm.StatusStop {
			break
		}
	}
}
