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
	e.ResultsDir = e.RunDir
	e.App = new(app.Info)
	e.App.Name = "test_app"
	e.Job = new(job.Job)
	e.Job.ID = 42
	e.Job.Name = "dummy_job"
	e.Job.BatchScript = "dummy_script"
	e.Job.Partition = "dummy_partition"
	e.MPICfg = new(MPIConfig)
	e.MPICfg.MPI = new(implem.Info)
	e.MPICfg.MPI.ID = "dummy_mpi"
	e.MPICfg.MPI.Version = "1.0.0"
	e.Platform = new(platform.Info)
	e.Platform.Name = "dummy_platform"

	e.Hash, err = e.toHash()
	if err == nil {
		t.Fatalf("unable to set experiment's hash: %s", err)
	}
	if e.Hash == "" {
		t.Fatalf("unable to set experiment's hash")
	}

	// No job log file exists, we should get 0
	n, err := e.getNumResults()
	if n != 0 || err != nil {
		t.Fatalf("e.getNumResults() returned %d instead of 0 (err: %s)", n, err)
	}

	// Create a job log file with a dummy entry
	filePath := filepath.Join(tempDir, "jobs.log")
	dummyHashRaw := sha256.Sum256([]byte("dummy"))
	dummyHash := fmt.Sprintf("%x", dummyHashRaw[:])
	dummyJobID := 1
	dummyJobName := "dummy"
	dummyBatchScript := "dummy.sh"
	content := fmt.Sprintf("%s %d %s %s\n", dummyHash, dummyJobID, dummyJobName, dummyBatchScript)
	err = ioutil.WriteFile(filePath, []byte(content), 0777)
	if err != nil {
		t.Fatalf("unable to add dummy entry to job log: %s", err)
	}

	n, err = e.getNumResults()
	if n != 0 || err != nil {
		t.Fatalf("e.getNumResults() returned %d instead of 0 (err=%s)", n, err)
	}

	err = e.addJobsToLog()
	if err != nil {
		t.Fatalf("e.addJobsToLog() failed: %s", err)
	}
	// Create dummy output files so we can mimic successful runs
	outputFile1 := filepath.Join(tempDir, e.Hash+"-dummy-20210615.out")
	err = ioutil.WriteFile(outputFile1, []byte("something"), 0777)
	if err != nil {
		t.Fatalf("unable to create dummy output file: %s", err)
	}

	n, err = e.getNumResults()
	if n != 1 || err != nil {
		t.Fatalf("e.getNumResults() returned %d instead of 1 (err=%s)", n, err)
	}

	err = e.addJobsToLog()
	if err != nil {
		t.Fatalf("e.addJobsToLog() failed: %s", err)
	}
	outputFile2 := filepath.Join(tempDir, e.Hash+"-dummy-20210616.out")
	err = ioutil.WriteFile(outputFile2, []byte("something else"), 0777)
	if err != nil {
		t.Fatalf("unable to create dummy output file: %s", err)
	}

	err = e.addJobsToLog()
	if err != nil {
		t.Fatalf("e.addJobsToLog() failed: %s", err)
	}
	outputFile3 := filepath.Join(tempDir, e.Hash+"-dummy-20210617.out")
	err = ioutil.WriteFile(outputFile3, []byte("something else again"), 0777)
	if err != nil {
		t.Fatalf("unable to create dummy output file: %s", err)
	}

	n, err = e.getNumResults()
	if n != 3 || err != nil {
		t.Fatalf("e.getNumResults() returned %d instead of 3", n)
	}
}

func TestManifestParsing(t *testing.T) {
	manifestContentExample := `*****************************
	d80ff0e7d124a6da455adf051d6a743a067f0babbd538cf567d401fb
	-mca coll_hcoll_enable 1 --mca opal_common_ucx_opal_mem_hooks 1
	iris
	
	32
	32
	
	
	/home/toto/workspace/install/ompi
	*****************************
	5099182dacdc34761affd0f586a6e1481b747b4443f7882a90d12fe9
	-mca coll_hcoll_enable 1 --mca opal_common_ucx_opal_mem_hooks 1 -x UCX_TLS=dc,knem,self
	iris

	32
	32


	/home/toto/workspace/install/ompi`

	data, err := parseManifestContent(strings.Split(manifestContentExample, "\n"))
	if err != nil {
		t.Fatalf("parseManifestContent() failed: %s", err)
	}

	expectedHash := "d80ff0e7d124a6da455adf051d6a743a067f0babbd538cf567d401fb"
	if _, ok := data[expectedHash]; !ok {
		t.Fatalf("no entry for hash %s", expectedHash)
	}
	curHash := data[expectedHash].Hash
	if curHash != expectedHash {
		t.Fatalf("invalid hash: %s instead of %s", curHash, expectedHash)
	}
	curMpiArgs := "-mca coll_hcoll_enable 1 --mca opal_common_ucx_opal_mem_hooks 1"
	if curMpiArgs != data[expectedHash].MpirunArgs {
		t.Fatalf("invalid mpiruna arguments: %s instead of %s", curMpiArgs, data[expectedHash].MpirunArgs)
	}

	expectedHash = "5099182dacdc34761affd0f586a6e1481b747b4443f7882a90d12fe9"
	curHash = data[expectedHash].Hash
	if curHash != expectedHash {
		t.Fatalf("invalid hash: %s instead of %s", curHash, expectedHash)
	}
	curMpiArgs = "-mca coll_hcoll_enable 1 --mca opal_common_ucx_opal_mem_hooks 1 -x UCX_TLS=dc,knem,self"
	if curMpiArgs != data[expectedHash].MpirunArgs {
		t.Fatalf("invalid mpiruna arguments: %s instead of %s", curMpiArgs, data[expectedHash].MpirunArgs)
	}

}
