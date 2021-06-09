// Copyright (c) 2019, Sylabs Inc. All rights reserved.
// Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
// This software is licensed under a 3-clause BSD license. Please consult the
// LICENSE.md file distributed with the sources of this project regarding your
// rights to use or distribute this software.

package experiments

import (
	"crypto/sha256"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gvallee/go_exec/pkg/advexec"
	"github.com/gvallee/go_exec/pkg/results"
	"github.com/gvallee/go_hpc_jobmgr/pkg/implem"
	"github.com/gvallee/go_hpc_jobmgr/pkg/jm"
	"github.com/gvallee/go_hpc_jobmgr/pkg/job"
	"github.com/gvallee/go_hpc_jobmgr/pkg/launcher"
	"github.com/gvallee/go_hpc_jobmgr/pkg/mpi"
	"github.com/gvallee/go_hpc_jobmgr/pkg/sys"
	"github.com/gvallee/go_software_build/pkg/app"
	"github.com/gvallee/go_software_build/pkg/buildenv"
	"github.com/gvallee/go_software_build/pkg/builder"
	"github.com/gvallee/go_util/pkg/util"
	"github.com/gvallee/validation_tool/pkg/platform"
)

type MPIConfig struct {
	// MPI holds all the details about the MPI implementation to use
	MPI *implem.Info

	// BuildEnv is the environment to use for the experiment(s)
	BuildEnv buildenv.Info
}

type ExperimentResult struct {
	Res               results.Result
	ExecRes           advexec.Result
	PostRunUpdateDone bool
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

	sysCfg *sys.Config

	// NumResults is the number of required results for the experiment
	NumResults int

	// hash represents the experiment so we have a unique and easy way to track experiments
	hash string
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

	// NumResults is the number of required results for the experiments
	NumResults int
}

type Runtime struct {
	count int

	wg *sync.WaitGroup

	pendingExperiments []*Experiment

	runningExperiments []*Experiment

	MaxRunningJobs int

	Started bool

	SleepBeforeSubmittingAgain time.Duration

	ProgressFrequency time.Duration
}

type SubmittedJob struct {
	ID     int
	Name   string
	Hash   string
	Script string
}

type Info struct {
	Name   string
	Dir    string
	Script string
}

const jobLogFilename = "jobs.log"

var jobLogs = []string{"jobs.log", "test.log"}

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

func (e *Experiment) toHash() string {
	if e.App == nil || e.Platform == nil {
		return ""
	}
	hashText := []string{fmt.Sprintf("%d", e.id), e.App.Name, e.RunDir, e.LaunchScript, e.Platform.Name, e.Platform.Device, fmt.Sprintf("%d", e.Platform.MaxNumNodes), fmt.Sprintf("%d", e.Platform.MaxPPR)}
	hashText = append(hashText, e.MpirunArgs...)
	if e.MPICfg != nil {
		hashText = append(hashText, e.MPICfg.MPI.ID)
		hashText = append(hashText, e.MPICfg.MPI.Version)
	}
	hash := sha256.Sum224([]byte(strings.Join(hashText, "\n")))
	// Sprintf ensures we get a string with standard characters that can be used in file names
	return fmt.Sprintf("%x", hash)
}

func parseJobLogFile(path string) ([]SubmittedJob, error) {
	var jobList []SubmittedJob
	content, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(string(content), "\n")
	for _, line := range lines {
		tokens := strings.Split(line, " ")
		if len(tokens) > 2 {
			jobID, err := strconv.Atoi(tokens[1])
			if err != nil {
				return nil, err
			}
			var newJob = SubmittedJob{
				ID:     jobID,
				Hash:   tokens[0],
				Name:   tokens[1],
				Script: tokens[2],
			}
			jobList = append(jobList, newJob)
		}
	}
	return jobList, nil
}

func (e *Experiment) addManifest() error {
	manifestFileName := "experiments.MANIFEST"
	content := "*****************************"
	content += e.hash + "\n" + strings.Join(e.MpirunArgs, " ") + "\n"
	if e.Platform != nil {
		content += e.Platform.Name + "\n"
		content += e.Platform.Device + "\n"
		content += fmt.Sprintf("%d\n", e.Platform.MaxNumNodes)
		content += fmt.Sprintf("%d\n", e.Platform.MaxPPR)
	}
	if e.MPICfg != nil && e.MPICfg.MPI != nil {
		content += e.MPICfg.MPI.ID + "\n" + e.MPICfg.MPI.Version + "\n" + e.MPICfg.BuildEnv.InstallDir + "\n"
	}
	manifestPath := filepath.Join(e.RunDir, manifestFileName)
	if util.FileExists(manifestPath) {
		data, err := ioutil.ReadFile(manifestPath)
		if err != nil {
			return err
		}
		content += string(data)
	}

	err := ioutil.WriteFile(manifestPath, []byte(content), 0644)
	if err != nil {
		return err
	}

	return nil
}

func (e *Experiment) addJobsToLog() error {
	if e.hash == "" {
		e.hash = e.toHash()
		if e.hash == "" {
			return fmt.Errorf("unable to get experiment's hash")
		}
	}
	content := fmt.Sprintf("%s %d %s %s\n", e.hash, e.job.ID, e.job.Name, e.job.BatchScript)
	jobLogFile := filepath.Join(e.RunDir, jobLogFilename)

	if util.FileExists(jobLogFile) {
		data, err := ioutil.ReadFile(jobLogFile)
		if err != nil {
			return err
		}
		content += string(data)
	}

	err := ioutil.WriteFile(jobLogFile, []byte(content), 0644)
	if err != nil {
		return err
	}
	return nil
}

func NewRuntime() *Runtime {
	r := new(Runtime)
	r.wg = new(sync.WaitGroup)
	r.count = 0
	r.MaxRunningJobs = 1
	r.SleepBeforeSubmittingAgain = 10
	r.ProgressFrequency = 1
	return r
}

/*
func processOutput(execRes *advexec.Result, expRes *results.Result, appInfo *app.Info) error {
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

	return nil
}
*/

func (r *Runtime) triggerExperiment() error {
	var expMPICfg mpi.Config
	var err error

	if len(r.pendingExperiments) == 0 {
		return nil
	}

	e := r.pendingExperiments[0]
	e.Result = new(ExperimentResult)

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
	e.sysCfg = &sysCfg

	if e.Env != nil {
		b.Env.ScratchDir = e.Env.ScratchDir
		b.Env.InstallDir = e.Env.InstallDir
		b.Env.BuildDir = e.Env.BuildDir
		b.App.Name = e.App.Name
		b.App.URL = e.App.URL

		if !util.FileExists(b.App.BinPath) {
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
			log.Printf("Application's binary already available, no need to build it")
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
		if e.MPICfg.MPI == nil {
			// if MPICfg is not nil, MPICfg.MPI should not be nil
			e.Result.ExecRes.Err = fmt.Errorf("MPI configuration is invalid")
			e.Result.Res.Pass = false
			log.Printf("%s", e.Result.ExecRes.Err)
			goto ExpCompleted
		}
		if e.MPICfg.MPI.InstallDir != "" {
			expMPICfg.Implem.InstallDir = e.MPICfg.MPI.InstallDir
		} else {
			expMPICfg.Implem.InstallDir = e.MPICfg.BuildEnv.InstallDir
		}
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
		e.job.NNodes = e.Platform.MaxNumNodes
		e.job.NP = e.Platform.MaxPPR * e.Platform.MaxNumNodes
		e.job.Device = e.Platform.Device
	}
	if e.App != nil {
		e.job.App.Name = e.App.BinName
		e.job.App.BinArgs = e.App.BinArgs
		e.job.App.BinName = e.App.BinName
		e.job.App.BinPath = e.App.BinPath
		e.job.Name = e.hash
	}
	e.job.RunDir = e.RunDir
	e.job.NonBlocking = true
	if e.LaunchScript != "" {
		e.job.BatchScript = e.LaunchScript
	}
	if len(e.RequiredModules) > 0 {
		e.job.RequiredModules = e.RequiredModules
	}

	log.Printf("Launching experiment %d\n", e.id)
	e.Result.Res, e.Result.ExecRes = launcher.Run(e.job, &expMPICfg, e.jobMgr, &sysCfg, nil)
	if err != nil {
		e.Result.ExecRes.Err = fmt.Errorf("failed to submit experiment: %s", e.Result.ExecRes.Err)
		e.Result.Res.Pass = false
		goto ExpCompleted
	}

	err = e.addJobsToLog()
	if err != nil {
		e.Result.ExecRes.Err = fmt.Errorf("failed to update job log: %s", err)
		e.Result.Res.Pass = false
		goto ExpCompleted
	}

	/*
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
	*/

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

func (e *Experiment) postRunUpdate() error {
	if e.jobMgr == nil || e.sysCfg == nil || e.Result == nil {
		// The experiment is defined but not yet submitted
		return fmt.Errorf("job has not completed yet")
	}
	if e.Result.PostRunUpdateDone {
		// Already done
		return nil
	}
	res := e.jobMgr.PostRun(&e.Result.ExecRes, e.job, e.sysCfg)
	e.Result.ExecRes = res
	e.Result.PostRunUpdateDone = true
	return nil
}

func (r *Runtime) checkCompletions(idx int) {

	if len(r.runningExperiments) < idx+1 {
		return
	}

	s := r.runningExperiments[idx].getStatus()
	if s == jm.StatusDone || s == jm.StatusStop {
		log.Printf("Experiment %d has completed\n", r.runningExperiments[idx].id)
		if idx == 0 {
			r.runningExperiments = r.runningExperiments[1:]
		} else {
			r.runningExperiments = append(r.runningExperiments[:idx], r.runningExperiments[idx+1:]...)
		}
		r.checkCompletions(idx)
		return
	}

	if idx+1 < len(r.runningExperiments) {
		r.checkCompletions(idx + 1)
	}
}

func (r *Runtime) startExperiment() error {
	if len(r.pendingExperiments) > 0 {
		if len(r.runningExperiments) < r.MaxRunningJobs || r.MaxRunningJobs == 0 {
			err := r.triggerExperiment()
			if err != nil {
				log.Printf("Triggering event failed: %s", err)
				return err
			}
			if len(r.pendingExperiments) > 0 && len(r.runningExperiments) < r.MaxRunningJobs {
				err = r.serveJobQueue()
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (r *Runtime) serveJobQueue() error {
	log.Printf("%d experiments are pending\n", len(r.pendingExperiments))
	err := r.startExperiment()
	if err != nil {
		return err
	}

	// Check for completion
	log.Printf("%d experiments are running\n", len(r.runningExperiments))
	r.checkCompletions(0)
	return nil
}

func getRunsFromLogFiles(dir string) ([]SubmittedJob, error) {
	var jobs []SubmittedJob
	var err error

	jobsLogFile := filepath.Join(dir, jobLogs[0])
	testLogFile := filepath.Join(dir, jobLogs[1])

	if !util.PathExists(jobsLogFile) && !util.PathExists(testLogFile) {
		// This is not an error, the log file does not exist because no jobs were submitted
		return nil, nil
	}

	if util.FileExists(jobsLogFile) {
		jobs, err = parseJobLogFile(jobsLogFile)
		if err != nil {
			return nil, fmt.Errorf("unable to parse job log file %s: %s", jobsLogFile, err)
		}
	}

	if util.FileExists(testLogFile) {
		if len(jobs) == 0 {
			jobs, err = parseJobLogFile(testLogFile)
			if err != nil {
				return nil, fmt.Errorf("unable to parse job log file %s: %s", testLogFile, err)
			}
		} else {
			morejobs, err := parseJobLogFile(testLogFile)
			if err != nil {
				return nil, fmt.Errorf("unable to parse job log file %s: %s", testLogFile, err)
			}
			jobs = append(jobs, morejobs...)
		}
	}

	return jobs, nil
}

func (e *Experiment) getNumResults() int {
	count := 0
	pastJobs, err := getRunsFromLogFiles(e.RunDir)
	if err != nil {
		log.Printf("unable to parse log file: %s", err)
		return -1
	}
	for _, j := range pastJobs {
		if j.Hash == e.hash {
			count++
		}
	}
	return count
}

func (e *Experiment) Run(r *Runtime) error {
	if r == nil {
		return fmt.Errorf("runtime is undefined")
	}

	e.id = -1
	if e.App == nil {
		return fmt.Errorf("application is undefined")
	}

	e.hash = e.toHash()
	if e.hash == "" {
		return fmt.Errorf("unable to get experiment's hash")
	}
	// At the moment, we cannot know how many of that experiment is queued or currently running
	// which is okay, we do not assume multiple runtimes handling the execution of the same
	// experiment
	nExistingResults := e.getNumResults()
	if nExistingResults == -1 {
		return fmt.Errorf("unable to get the number of existing results for experiment %d", e.id)
	}

	for i := nExistingResults; i < e.NumResults; i++ {
		e.runtime = r
		e.id = r.count
		r.count++
		err := e.addManifest()
		if err != nil {
			return fmt.Errorf("unable to add experiment's manifest: %s", err)
		}
		r.pendingExperiments = append(r.pendingExperiments, e)
		if !r.Started {
			r.Start()
		}
	}

	return nil
}

func (e *Experiments) Run(r *Runtime) error {
	if r == nil {
		return fmt.Errorf("runtime is nil")
	}

	log.Printf("%d experiments to run", len(e.List))
	for _, exp := range e.List {
		if e.App != nil && exp.App == nil {
			exp.App = e.App
		}
		if e.Env != nil && exp.Env == nil {
			exp.Env = e.Env
		}
		if e.Platform != nil && exp.Platform == nil {
			exp.Platform = e.Platform
		}
		if len(e.RequiredModules) > 0 {
			exp.RequiredModules = e.RequiredModules
		}
		if e.MPICfg != nil {
			exp.MPICfg = e.MPICfg
		}
		if e.RunDir != "" {
			exp.RunDir = e.RunDir
			/*
				// If RunDir is defined, we can track how many results we already have.
				// Figure out how many runs are required to fullfill the number of requested results
				jobs, err := getRunsFromLogFiles(exp.RunDir)
				if err != nil {
					fmt.Printf("runs.GetFromLogFiles() failed: %s\n", err)
					os.Exit(1)
				}
			*/
		}
		// We always need at least one result
		if e.NumResults == 0 {
			e.NumResults = 1
		}
		exp.NumResults = e.NumResults

		err := exp.Run(r)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *Runtime) Start() {
	if !r.Started {
		// Start the go routine that handles the job queue
		go func(r *Runtime) {
			if r == nil {
				log.Printf("runtime is nil")
				return
			}
			r.wg.Add(1)

			defer r.wg.Done()
			// fixme: do this only if the runtime routine is not created yet
			for len(r.pendingExperiments) > 0 || len(r.runningExperiments) > 0 {
				err := r.serveJobQueue()
				if err != nil {
					log.Printf("unable to run experiments: %s", err)
					break
				}
				if len(r.pendingExperiments) > 0 {
					// Some jobs could be submitted right away, so we wait 10 minutes
					time.Sleep(r.SleepBeforeSubmittingAgain * time.Minute)
				}
			}
			r.Started = false
		}(r)
	}
	r.Started = true
}

func (r *Runtime) Fini() {
	if r.wg == nil {
		return
	}
	r.wg.Wait()
}

// Wait makes the current process wait for the termination of the webUI
func (r *Runtime) Wait() {
	r.wg.Wait()
}

func (e *Experiment) Wait() {
	if e.runtime == nil {
		log.Println("undefined runtime")
		return
	}

	for e.Result == nil {
		time.Sleep(e.runtime.ProgressFrequency * time.Minute)
		s := e.getStatus()
		if s == jm.StatusDone || s == jm.StatusStop {
			err := e.postRunUpdate()
			if err != nil {
				log.Printf("postRunUpdate() failed")
				return
			}
			break
		}
	}
}

func (exps *Experiments) Wait(runtime *Runtime) {
	completed := 0
	for completed != len(exps.List) {
		for _, e := range exps.List {
			if e.Result != nil && !e.Result.PostRunUpdateDone {
				eStatus := e.getStatus()
				if eStatus == jm.StatusDone || eStatus == jm.StatusStop {
					completed++
					err := e.postRunUpdate()
					if err != nil {
						log.Printf("postRunUpdate() failed: %s", err)
						return
					}
					// Exit early
					if completed == len(exps.List) {
						return
					}
				}
			}
		}
		time.Sleep(runtime.ProgressFrequency * time.Minute)
	}
}
