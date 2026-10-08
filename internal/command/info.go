package command

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"sort"
	"strconv"
	"strings"

	"github.com/maltemindedal/stash/internal/protocol"
	"github.com/maltemindedal/stash/internal/server"
	"github.com/maltemindedal/stash/internal/storage"
)

func (e *Executor) handleInfo(_ context.Context, request *Request) (protocol.Value, error) {
	sections := []string{"memory", "replication", "clients", "persistence"}
	if len(request.Args) == 1 {
		section := strings.ToLower(string(request.Args[0]))
		switch section {
		case "default", "all":
			// Keep all implemented sections.
		case "memory", "replication", "clients", "persistence":
			sections = []string{section}
		default:
			return nil, ErrSyntaxError()
		}
	}

	var buf bytes.Buffer
	for i, section := range sections {
		if i > 0 {
			buf.WriteString("\r\n")
		}
		switch section {
		case "memory":
			e.appendInfoMemory(&buf)
		case "replication":
			e.appendInfoReplication(&buf)
		case "clients":
			e.appendInfoClients(&buf)
		case "persistence":
			e.appendInfoPersistence(&buf)
		}
	}

	return protocol.TextBulkString{Value: buf.String()}, nil
}

func validateInfoRequest(request *Request) error {
	if len(request.Args) > 1 {
		return wrongNumberOfArgumentsError("INFO")
	}
	return nil
}

func (e *Executor) appendInfoMemory(buf *bytes.Buffer) {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	keyStats := storage.KeyStats{ByKind: make(map[storage.ValueKind]int)}
	if e.store != nil {
		keyStats = e.store.KeyStats()
	}
	usedMemory := int64(0)
	maxMemory := int64(0)
	if e.store != nil {
		usedMemory = e.store.UsedMemory()
		maxMemory = e.store.MaxMemory()
	}

	buf.WriteString("# Memory\r\n")
	appendInfoField(buf, "used_memory", usedMemory)
	appendInfoField(buf, "used_memory_human", humanBytes(uint64(max(usedMemory, 0))))
	appendInfoField(buf, "maxmemory", maxMemory)
	appendInfoField(buf, "maxmemory_human", humanBytes(uint64(max(maxMemory, 0))))
	appendInfoField(buf, "mem_fragmentation_ratio", "1.00")
	appendInfoField(buf, "go_heap_alloc", mem.HeapAlloc)
	appendInfoField(buf, "go_heap_sys", mem.HeapSys)
	appendInfoField(buf, "go_heap_idle", mem.HeapIdle)
	appendInfoField(buf, "key_count", keyStats.TotalKeys)

	kinds := []storage.ValueKind{
		storage.ValueKindString,
		storage.ValueKindList,
		storage.ValueKindHash,
		storage.ValueKindSet,
		storage.ValueKindZSet,
		storage.ValueKindStream,
	}
	for _, kind := range kinds {
		appendInfoField(buf, "key_count_"+string(kind), keyStats.ByKind[kind])
	}
}

func (e *Executor) appendInfoReplication(buf *bytes.Buffer) {
	stats := e.serverStats()

	buf.WriteString("# Replication\r\n")
	appendInfoField(buf, "role", stats.Role)
	appendInfoField(buf, "master_replid", stats.MasterReplicationID)
	appendInfoField(buf, "master_repl_offset", stats.MasterOffset)
	appendInfoField(buf, "slave_repl_offset", stats.ReplicaOffset)
	appendInfoField(buf, "connected_slaves", len(stats.Replicas))

	replicas := append([]server.ReplicaInfo(nil), stats.Replicas...)
	sort.Slice(replicas, func(i, j int) bool { return replicas[i].ID < replicas[j].ID })
	for i, replica := range replicas {
		appendInfoField(buf, fmt.Sprintf("slave%d", i), fmt.Sprintf("id=%d,port=%d,offset=%d", replica.ID, replica.ListeningPort, replica.AckedMasterOffset))
	}
}

func (e *Executor) appendInfoClients(buf *bytes.Buffer) {
	stats := e.serverStats()

	buf.WriteString("# Clients\r\n")
	appendInfoField(buf, "connected_clients", stats.ConnectedClients)
	appendInfoField(buf, "monitoring_clients", stats.MonitoringClients)
	appendInfoField(buf, "total_commands_processed", stats.CommandsProcessed)
}

func (e *Executor) appendInfoPersistence(buf *bytes.Buffer) {
	stats := e.serverStats()

	status := "ok"
	if !stats.AOFLastWriteOK {
		status = "err"
	}

	buf.WriteString("# Persistence\r\n")
	appendInfoField(buf, "aof_enabled", boolToInt(stats.AOFEnabled))
	appendInfoField(buf, "aof_last_write_status", status)
}

func boolToInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func (e *Executor) serverStats() server.Stats {
	if e.serverStatsProvider == nil {
		return server.Stats{Role: "master", AOFLastWriteOK: true}
	}

	return e.serverStatsProvider()
}

func appendInfoField(buf *bytes.Buffer, name string, value any) {
	buf.WriteString(name)
	buf.WriteByte(':')
	buf.WriteString(infoValueString(value))
	buf.WriteString("\r\n")
}

func infoValueString(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case int:
		return strconv.Itoa(typed)
	case int64:
		return strconv.FormatInt(typed, 10)
	case uint64:
		return strconv.FormatUint(typed, 10)
	default:
		return fmt.Sprint(typed)
	}
}

func humanBytes(value uint64) string {
	const unit = 1024
	if value < unit {
		return fmt.Sprintf("%dB", value)
	}
	divisor := uint64(unit)
	unitIndex := 0
	for n := value / unit; n >= unit && unitIndex < 4; n /= unit {
		divisor *= unit
		unitIndex++
	}
	return fmt.Sprintf("%.2f%c", float64(value)/float64(divisor), "KMGTP"[unitIndex])
}
