// Copyright 2014 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package utils

import (
	"context"
	"os"
	"os/exec"
	"time"
	"log"
)

func FileExists(file string) bool {
	if IsFilesystemHung(file) {
		return false
	}
	if _, err := os.Stat(file); err != nil {
		return false
	}
	return true
}
func IsFilesystemHung(path string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	stat, err := exec.LookPath("stat")
	if err != nil {
		return false
	}
	// we don't actually care if the command suceeds or fails, just if it hangs
	_ = exec.CommandContext(ctx, stat, path).Run()
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		log.Printf("Filesystem where %s resides appears hung", path)
		return true
	}
	return false
}

