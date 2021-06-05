// Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
// This software is licensed under a 3-clause BSD license. Please consult the
// LICENSE.md file distributed with the sources of this project regarding your
// rights to use or distribute this software.

package experiments

import (
	"crypto/sha256"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gvallee/go_hpc_jobmgr/pkg/implem"
	"github.com/gvallee/go_hpc_jobmgr/pkg/job"
	"github.com/gvallee/go_software_build/pkg/app"
	"github.com/gvallee/go_util/pkg/util"
	"github.com/gvallee/validation_tool/pkg/platform"
)

var partition = flag.String("partition", "", "Name of Slurm partition to use to run the test")
var modules = flag.String("modules", "", "Comma-separated list of modules to use to run the test")
var runDir = flag.String("run-dir", "", "From where the test needs to be executed")

//var mpiInstallDir = flag.String("mpi-dir", "", "MPI install directory to use to execute the test")

func setExperiment(t *testing.T) *Experiment {
	var err error
	dummyApp := new(app.Info)
	dummyApp.Name = "date"
	dummyApp.BinName = "date"
	dummyApp.BinPath, err = exec.LookPath("date")
	if err != nil {
		t.Fatalf("unable to find date binary: %s", err)
	}

	targetPlatform := new(platform.Info)
	targetPlatform.Name = *partition
	targetPlatform.MaxNumNodes = 2
	targetPlatform.MaxPPR = 1

	e := new(Experiment)
	e.App = dummyApp
	e.Platform = targetPlatform
	if *modules != "" {
		e.RequiredModules = strings.Split(*modules, ",")
	}
	e.RunDir = *runDir
	if !util.PathExists(e.RunDir) {
		t.Fatalf("%s does not exist", e.RunDir)
	}

	return e
}

func TestRunSingle(t *testing.T) {
	if !util.PathExists(*runDir) {
		t.Skip("run directory not specified, skipping")
	}

	r := NewRuntime()

	e := setExperiment(t)
	e.MPICfg = new(MPIConfig)
	/*
		e.MPICfg.BuildEnv.InstallDir = *mpiInstallDir
		if !util.PathExists(e.MPICfg.BuildEnv.InstallDir) {
			t.Fatalf("%s does not exist", e.MPICfg.BuildEnv.InstallDir)
		}
	*/
	err := e.Run(r)
	if err != nil {
		t.Fatalf("experiment failed: %s", err)
	}
	e.Wait()

	fmt.Printf("Output: %s\n", e.Result.ExecRes.Stdout)
	r.Fini()
}

func TestRunExperiments(t *testing.T) {
	if !util.PathExists(*runDir) {
		t.Skip("run directory not specified, skipping")
	}

	r := NewRuntime()
	r.SleepBeforeSubmittingAgain = 1

	exps := new(Experiments)
	exps.MPICfg = new(MPIConfig)

	// We cannot queue too many job as a Go test as a 10 minutes timeout.
	for i := 0; i < 4; i++ {
		e := setExperiment(t)
		exps.List = append(exps.List, e)
	}

	err := exps.Run(r)
	if err != nil {
		t.Fatalf("unable to submit experiments")
	}

	exps.Wait(r)
	r.Fini()
}

func TestGetNumResults(t *testing.T) {
	// Define a dummy experiment and simulate adding it to a log file
	tempDir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatalf("unable to create temporary directory: %s", err)
	}
	defer os.RemoveAll(tempDir)

	e := new(Experiment)
	e.id = 0
	e.RunDir = tempDir
	e.App = new(app.Info)
	e.App.Name = "test_app"
	e.job = new(job.Job)
	e.job.ID = 42
	e.job.Name = "dummy_job"
	e.job.BatchScript = "dummy_script"
	e.job.Partition = "dummy_partition"
	e.MPICfg = new(MPIConfig)
	e.MPICfg.MPI = new(implem.Info)
	e.MPICfg.MPI.ID = "dummy_mpi"
	e.MPICfg.MPI.Version = "1.0.0"

	e.hash = e.toHash()

	// No job log file exists, we should get 0
	n := e.getNumResults()
	if n != 0 {
		t.Fatalf("e.getNumResults() returned %d instead of 0", n)
	}

	// Create a job log file with a dummy entry
	filePath := filepath.Join(tempDir, "jobs.log")
	dummyHash := sha256.Sum256([]byte("dummy"))
	dummyJobID := 1
	dummyJobName := "dummy"
	dummyBatchScript := "dummy.sh"
	content := fmt.Sprintf("%s %d %s %s\n", string(dummyHash[:]), dummyJobID, dummyJobName, dummyBatchScript)
	err = ioutil.WriteFile(filePath, []byte(content), 0777)
	if err != nil {
		t.Fatalf("unable to add dummy entry to job log: %s", err)
	}

	n = e.getNumResults()
	if n != 0 {
		t.Fatalf("e.getNumResults() returned %d instead of 0", n)
	}

	err = e.addJobsToLog()
	if err != nil {
		t.Fatalf("e.addJobsToLog() failed: %s", err)
	}

	n = e.getNumResults()
	if n != 1 {
		t.Fatalf("e.getNumResults() returned %d instead of 1", n)
	}

	err = e.addJobsToLog()
	if err != nil {
		t.Fatalf("e.addJobsToLog() failed: %s", err)
	}

	err = e.addJobsToLog()
	if err != nil {
		t.Fatalf("e.addJobsToLog() failed: %s", err)
	}

	n = e.getNumResults()
	if n != 3 {
		t.Fatalf("e.getNumResults() returned %d instead of 3", n)
	}
}
