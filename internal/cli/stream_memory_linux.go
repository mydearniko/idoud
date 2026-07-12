//go:build linux

package cli

import (
	"bufio"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func platformStreamMemoryAvailable() int64 {
	host := linuxMemAvailable()
	cgroup := linuxCgroupMemoryAvailable()
	return minimumPositiveInt64(host, cgroup)
}

func linuxMemAvailable() int64 {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 2 || fields[0] != "MemAvailable:" {
			continue
		}
		kb, err := strconv.ParseInt(fields[1], 10, 64)
		if err == nil && kb > 0 {
			return kb * 1024
		}
	}
	return 0
}

func linuxCgroupMemoryAvailable() int64 {
	// cgroup v2. Use the process-relative path first, then the common root path.
	v2Paths := []string{"/sys/fs/cgroup"}
	if data, err := os.ReadFile("/proc/self/cgroup"); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			parts := strings.SplitN(line, ":", 3)
			if len(parts) == 3 && parts[0] == "0" && parts[1] == "" {
				rel := strings.TrimPrefix(filepath.Clean("/"+parts[2]), "/")
				v2Paths = append([]string{filepath.Join("/sys/fs/cgroup", rel)}, v2Paths...)
				break
			}
		}
	}
	for _, base := range v2Paths {
		if available := cgroupLimitAvailable(filepath.Join(base, "memory.max"), filepath.Join(base, "memory.current")); available > 0 {
			return available
		}
	}

	// cgroup v1, including a process-relative memory controller path.
	v1Paths := []string{"/sys/fs/cgroup/memory"}
	if data, err := os.ReadFile("/proc/self/cgroup"); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			parts := strings.SplitN(line, ":", 3)
			if len(parts) != 3 || !containsCSVField(parts[1], "memory") {
				continue
			}
			rel := strings.TrimPrefix(filepath.Clean("/"+parts[2]), "/")
			v1Paths = append([]string{filepath.Join("/sys/fs/cgroup/memory", rel)}, v1Paths...)
			break
		}
	}
	for _, base := range v1Paths {
		if available := cgroupLimitAvailable(filepath.Join(base, "memory.limit_in_bytes"), filepath.Join(base, "memory.usage_in_bytes")); available > 0 {
			return available
		}
	}
	return 0
}

func containsCSVField(raw, want string) bool {
	for _, field := range strings.Split(raw, ",") {
		if strings.TrimSpace(field) == want {
			return true
		}
	}
	return false
}

func cgroupLimitAvailable(limitPath, usedPath string) int64 {
	limitRaw, err := os.ReadFile(limitPath)
	if err != nil {
		return 0
	}
	limitText := strings.TrimSpace(string(limitRaw))
	if limitText == "" || limitText == "max" {
		return 0
	}
	limit, err := strconv.ParseInt(limitText, 10, 64)
	if err != nil || limit <= 0 || limit >= 1<<60 {
		return 0
	}
	usedRaw, err := os.ReadFile(usedPath)
	if err != nil {
		return 0
	}
	used, err := strconv.ParseInt(strings.TrimSpace(string(usedRaw)), 10, 64)
	if err != nil || used < 0 {
		return 0
	}
	if used >= limit {
		return 1
	}
	return limit - used
}
