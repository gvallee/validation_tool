// Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
// This software is licensed under a 3-clause BSD license. Please consult the
// LICENSE.md file distributed with the sources of this project regarding your
// rights to use or distribute this software.

package results

import (
	"io/ioutil"
	"path/filepath"
	"strings"

	"github.com/gvallee/go_util/pkg/util"
)

type ResultFiles struct {
	SuccessfulExperiments []string
	FailedExperiments     []string
}

func failed(dir string, filePrefix string) (bool, error) {
	errFile := filepath.Join(dir, filePrefix+".err")
	if !util.FileExists(errFile) {
		return false, nil
	}

	content, err := ioutil.ReadFile(errFile)
	if err != nil {
		return true, err
	}

	// Slurm error: job canceled
	if strings.Contains(string(content), "slurmstepd") && strings.Contains(string(content), "CANCELLED AT") {
		return true, err
	}

	return false, nil
}

func GetFiles(dir string) (*ResultFiles, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	resultFiles := new(ResultFiles)
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".out") || strings.HasSuffix(f.Name(), ".err") {
			filePrefix := strings.TrimSuffix(f.Name(), ".out")
			filePrefix = strings.TrimSuffix(filePrefix, ".err")
			failed, err := failed(dir, filePrefix)
			if err != nil {
				return nil, err
			}
			filePath := filepath.Join(dir, f.Name())
			if failed && strings.HasSuffix(f.Name(), ".err") {
				resultFiles.FailedExperiments = append(resultFiles.FailedExperiments, filePath)
			}
			if !failed && strings.HasSuffix(f.Name(), ".out") {
				resultFiles.SuccessfulExperiments = append(resultFiles.SuccessfulExperiments, filePath)
			}
		}
	}

	return resultFiles, nil
}
