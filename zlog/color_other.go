//go:build !windows

package zlog

import "os"

func enableColors(*os.File) bool { return true }
