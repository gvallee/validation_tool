// Copyright (c) 2019, Sylabs Inc. All rights reserved.
// Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
// This software is licensed under a 3-clause BSD license. Please consult the
// LICENSE.md file distributed with the sources of this project regarding your
// rights to use or distribute this software.

package experiments

import (
	"fmt"
	"log"

	"github.com/gvallee/go_exec/pkg/advexec"
	"github.com/gvallee/go_exec/pkg/results"
	"github.com/gvallee/go_hpc_jobmgr/pkg/implem"
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

type Experiment struct {
	// App gathers all the data about the application to include in the container
	App *app.Info

	// Result gathers all the data related to the result of an experiment
	Result results.Result

	// MpirunArgs is a list of mpirun arguments to use for the experiment
	MpirunArgs []string

	// Env gathers all the data related to how software has been installed
	Env *buildenv.Info

	// Platform gathers all the data required to execute experiments on a target platform
	Platform *platform.Info
}

type Info struct {
	MPICfg *MPIConfig

	// App gathers all the data about the application to include in the container
	App *app.Info

	List []*Experiment

	// Result gathers all the data related to the result of an experiment
	Result results.Result

	// Env gathers all the data related to how software has been installed
	Env *buildenv.Info

	// Platform gathers all the data required to execute experiments on a target platform
	Platform *platform.Info
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

func (e *Experiment) Run(mpiCfg *MPIConfig) (bool, results.Result, advexec.Result) {
	var expRes results.Result
	var execRes advexec.Result

	b := new(builder.Builder)
	b.Env.ScratchDir = e.Env.ScratchDir
	b.Env.InstallDir = e.Env.InstallDir
	b.Env.BuildDir = e.Env.BuildDir
	b.App.Name = e.App.Name
	b.App.URL = e.App.URL

	err := b.Load(true)
	if err != nil {
		execRes.Err = fmt.Errorf("unable to load a builder: %s", err)
		expRes.Pass = false
		return false, expRes, execRes
	}
	res := b.Install()
	if res.Err != nil {
		execRes.Err = fmt.Errorf("unable to install the experiment software: %s", res.Err)
		expRes.Pass = false
		return false, expRes, execRes
	}

	sysCfg, jobMgr, err := launcher.Load()
	if err != nil {
		execRes.Err = fmt.Errorf("unable to load a launcher: %s", err)
		expRes.Pass = false
		return false, expRes, execRes
	}
	sysCfg.ScratchDir = e.Env.ScratchDir
	var expMPICfg mpi.Config
	expMPICfg.Implem.InstallDir = mpiCfg.BuildEnv.InstallDir
	expMPICfg.UserMpirunArgs = e.MpirunArgs // fixme: add the default args we get from config file
	err = expMPICfg.Implem.Load()
	if err != nil {
		execRes.Err = fmt.Errorf("unable to detect information about the MPI implementation to use: %s", err)
		expRes.Pass = false
		return false, expRes, execRes
	}

	var newjob job.Job
	newjob.Partition = e.Platform.Name
	newjob.App.Name = e.App.BinName
	newjob.App.BinArgs = e.App.BinArgs
	newjob.App.BinName = e.App.BinName
	newjob.App.BinPath = e.App.BinPath
	newjob.Name = e.App.Name
	expRes, execRes = launcher.Run(&newjob, &expMPICfg, &jobMgr, &sysCfg, nil)
	if !expRes.Pass {
		return false, expRes, execRes
	}
	if execRes.Err != nil {
		execRes.Err = fmt.Errorf("failed to run experiment: %s", execRes.Err)
		/*
			err = launcher.SaveErrorDetails(&exp.HostMPI, &myContainerMPICfg.Implem, sysCfg, &execRes)
			if err != nil {
				execRes.Err = fmt.Errorf("failed to save error details: %s", err)
			}
		*/
		expRes.Pass = false
		return false, expRes, execRes
	}
	log.Printf("* Successful run - Analysing data...")

	err = processOutput(&execRes, &expRes, e.App)
	if err != nil {
		execRes.Err = fmt.Errorf("failed to process output: %s", err)
		expRes.Pass = false
		return false, expRes, execRes
	}

	log.Println("-> Experiment successfully executed")
	log.Printf("* Experiment's note: %s", expRes.Note)

	expRes.Pass = true
	return false, expRes, execRes
}

func (e *Info) Run() bool {
	validationStatus := true
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
		pass, _, _ := exp.Run(e.MPICfg)
		if !pass && validationStatus {
			validationStatus = false
		}
	}
	return validationStatus
}
