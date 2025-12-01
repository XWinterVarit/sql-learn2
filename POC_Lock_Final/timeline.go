package main

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"time"
)

// TimelineEvent represents a single operation event (start, end, or commit)
type TimelineEvent struct {
	Flow      string // "CHAIN" or "EARLY"
	Table     string // "A", "B", "C", or "" for COMMIT
	EventType string // "START", "END", or "COMMIT"
	Time      time.Time
}

// TimelineTracker collects timeline events from multiple goroutines
type TimelineTracker struct {
	mu     sync.Mutex
	events []TimelineEvent
	start  time.Time
}

// NewTimelineTracker creates a new timeline tracker
func NewTimelineTracker(startTime time.Time) *TimelineTracker {
	return &TimelineTracker{
		events: make([]TimelineEvent, 0),
		start:  startTime,
	}
}

// RecordStart records the start of an operation
func (t *TimelineTracker) RecordStart(flow, table string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.events = append(t.events, TimelineEvent{
		Flow:      flow,
		Table:     table,
		EventType: "START",
		Time:      time.Now(),
	})
}

// RecordExpected records the expected start time of an operation
func (t *TimelineTracker) RecordExpected(flow, table string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.events = append(t.events, TimelineEvent{
		Flow:      flow,
		Table:     table,
		EventType: "EXPECTED",
		Time:      time.Now(),
	})
}

// RecordEnd records the end of an operation
func (t *TimelineTracker) RecordEnd(flow, table string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.events = append(t.events, TimelineEvent{
		Flow:      flow,
		Table:     table,
		EventType: "END",
		Time:      time.Now(),
	})
}

// RecordCommit records a transaction commit event
func (t *TimelineTracker) RecordCommit(flow string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.events = append(t.events, TimelineEvent{
		Flow:      flow,
		Table:     "", // No table for commit events
		EventType: "COMMIT",
		Time:      time.Now(),
	})
}

// RecordRollback records a transaction rollback event
func (t *TimelineTracker) RecordRollback(flow string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.events = append(t.events, TimelineEvent{
		Flow:      flow,
		Table:     "",
		EventType: "ROLLBACK",
		Time:      time.Now(),
	})
}

// Segment represents a time segment for a table operation
type Segment struct {
	Table string
	Start float64 // seconds from demo start
	End   float64 // seconds from demo start
}

// FlowTimeline represents all segments for one flow
type FlowTimeline struct {
	Flow     string
	Segments []Segment
}

// RenderTimeline generates and prints an ASCII timeline graph
func (t *TimelineTracker) RenderTimeline(showExpected bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(t.events) == 0 {
		fmt.Println("No timeline events recorded.")
		return
	}

	// Sort events by time
	sort.Slice(t.events, func(i, j int) bool {
		return t.events[i].Time.Before(t.events[j].Time)
	})

	// Build segments for each flow and collect commit/rollback times
	flowSegments := make(map[string][]Segment)
	commitTimes := make(map[string]float64)   // flow -> commit time in seconds
	rollbackTimes := make(map[string]float64) // flow -> rollback time in seconds
	pending := make(map[string]map[string]time.Time)

	for _, event := range t.events {
		// Handle EXPECTED events separately
		if event.EventType == "EXPECTED" {
			tSec := event.Time.Sub(t.start).Seconds()
			targetFlow := event.Flow + " EXPECTED"
			flowSegments[targetFlow] = append(flowSegments[targetFlow], Segment{
				Table: event.Table,
				Start: tSec,
				End:   tSec, // Point segment
			})
			continue
		}

		if pending[event.Flow] == nil {
			pending[event.Flow] = make(map[string]time.Time)
		}

		if event.EventType == "START" {
			pending[event.Flow][event.Table] = event.Time
		} else if event.EventType == "END" {
			if startTime, ok := pending[event.Flow][event.Table]; ok {
				startSec := startTime.Sub(t.start).Seconds()
				endSec := event.Time.Sub(t.start).Seconds()

				flowSegments[event.Flow] = append(flowSegments[event.Flow], Segment{
					Table: event.Table,
					Start: startSec,
					End:   endSec,
				})
				delete(pending[event.Flow], event.Table)
			}
		} else if event.EventType == "COMMIT" {
			commitTimes[event.Flow] = event.Time.Sub(t.start).Seconds()
		} else if event.EventType == "ROLLBACK" {
			rollbackTimes[event.Flow] = event.Time.Sub(t.start).Seconds()
		}
	}

	// Calculate total duration (find the latest event time, including commits)
	var maxTime time.Time
	for _, event := range t.events {
		if event.Time.After(maxTime) {
			maxTime = event.Time
		}
	}
	totalDuration := maxTime.Sub(t.start).Seconds()

	// Create ordered flow list
	var displayFlows []string
	if showExpected {
		displayFlows = []string{"CHAIN EXPECTED", "CHAIN", "EARLY EXPECTED", "EARLY", "NONTX EXPECTED", "NONTX"}
	} else {
		displayFlows = []string{"CHAIN", "EARLY", "TX"}
	}
	timelines := make([]FlowTimeline, 0)
	for _, flowName := range displayFlows {
		if segs, ok := flowSegments[flowName]; ok {
			// Sort segments by start time
			sort.SliceStable(segs, func(i, j int) bool {
				return segs[i].Start < segs[j].Start
			})
			timelines = append(timelines, FlowTimeline{
				Flow:     flowName,
				Segments: segs,
			})
		}
	}

	// Render timeline
	fmt.Println("\n=== Timeline Graph ===")
	fmt.Printf("Total duration: %.1f seconds\n\n", totalDuration)

	// Width of timeline in characters
	const timelineWidth = 60
	scale := float64(timelineWidth) / totalDuration

	for _, tl := range timelines {
		// Print flow name
		fmt.Printf("%-15s ", tl.Flow)

		// Create timeline array
		line := make([]rune, timelineWidth)
		for i := range line {
			line[i] = '-'
		}

		// Track the next available position to avoid overlaps
		nextPos := 0

		// Place segments
		for _, seg := range tl.Segments {
			if seg.Table == "SLEEP" {
				continue
			}
			startPos := int(math.Round(seg.Start * scale))
			endPos := int(math.Round(seg.End * scale))

			// Clamp to bounds and ensure visibility
			if startPos < 0 {
				startPos = 0
			}
			// Shift right if position is already occupied (estimating/padding)
			if startPos < nextPos {
				startPos = nextPos
			}

			// Ensure at least one character width if valid
			if endPos < startPos {
				endPos = startPos
			}

			if startPos >= timelineWidth {
				continue
			}
			if endPos >= timelineWidth {
				endPos = timelineWidth - 1
			}

			// Fill segment with table label
			label := []rune(seg.Table)
			for i := startPos; i <= endPos; i++ {
				labelIdx := (i - startPos) % len(label)
				line[i] = label[labelIdx]
			}

			// Update next available position
			nextPos = endPos + 1
		}

		// Place commit marker "X" if commit event exists for this flow
		if commitTime, ok := commitTimes[tl.Flow]; ok {
			commitPos := int(math.Round(commitTime * scale))
			// Allow placing X at the last position (timelineWidth - 1)
			if commitPos >= timelineWidth {
				commitPos = timelineWidth - 1
			}
			if commitPos >= 0 && commitPos < timelineWidth {
				line[commitPos] = 'X'
			}
		}

		// Place rollback marker "R" if rollback event exists for this flow
		if rollbackTime, ok := rollbackTimes[tl.Flow]; ok {
			rollbackPos := int(math.Round(rollbackTime * scale))
			if rollbackPos >= timelineWidth {
				rollbackPos = timelineWidth - 1
			}
			if rollbackPos >= 0 && rollbackPos < timelineWidth {
				line[rollbackPos] = 'R'
			}
		}

		// Print timeline
		fmt.Println(string(line))
	}

	// Print time scale
	fmt.Printf("                ")
	for i := 0; i <= 6; i++ {
		t := totalDuration * float64(i) / 6.0
		fmt.Printf("%-10s", fmt.Sprintf("%.1fs", t))
	}
	fmt.Println()
}
