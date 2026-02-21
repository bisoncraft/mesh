package main

import (
	"fmt"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
)

const sectionMaxVisible = 10

// detailSection holds the content lines for one scrollable section.
type detailSection struct {
	title      string
	lines      []string
	keys       []string // parallel to lines; enables item selection when non-empty
	offset     int
	itemCursor int // highlighted item index (only used when keys is non-empty)
}

func (s *detailSection) scrollDown() {
	max := len(s.lines) - sectionMaxVisible
	if max < 0 {
		max = 0
	}
	if s.offset < max {
		s.offset++
	}
}

func (s *detailSection) scrollUp() {
	if s.offset > 0 {
		s.offset--
	}
}

func (s *detailSection) cursorDown() {
	// Skip lines with empty keys to only land on selectable items.
	for next := s.itemCursor + 1; next < len(s.lines); next++ {
		if next < len(s.keys) && s.keys[next] != "" {
			s.itemCursor = next
			break
		}
	}
	if s.itemCursor >= s.offset+sectionMaxVisible {
		s.offset = s.itemCursor - sectionMaxVisible + 1
	}
}

func (s *detailSection) cursorUp() {
	// Skip lines with empty keys to only land on selectable items.
	for prev := s.itemCursor - 1; prev >= 0; prev-- {
		if prev < len(s.keys) && s.keys[prev] != "" {
			s.itemCursor = prev
			break
		}
	}
	if s.itemCursor < s.offset {
		s.offset = s.itemCursor
	}
}

func (s detailSection) selectedKey() string {
	if len(s.keys) == 0 || s.itemCursor >= len(s.keys) {
		return ""
	}
	return s.keys[s.itemCursor]
}

func (s detailSection) needsScroll() bool {
	return len(s.lines) > sectionMaxVisible
}

func (s detailSection) visibleLines() []string {
	if !s.needsScroll() {
		return s.lines
	}
	end := s.offset + sectionMaxVisible
	if end > len(s.lines) {
		end = len(s.lines)
	}
	return s.lines[s.offset:end]
}

// renderSection renders a section with header, separator, scroll indicators,
// and content. Sections with keys get cursor highlighting on the selected item.
func renderSection(b *strings.Builder, sec *detailSection, focused bool) {
	// Section header
	titleStr := sec.title
	if focused {
		titleStr = cursorStyle.Render("\u25b6 ") + headerStyle.Render(sec.title)
	} else {
		titleStr = dimStyle.Render("  " + sec.title)
	}
	b.WriteString(" " + titleStr)

	// Scroll position indicator
	if sec.needsScroll() {
		b.WriteString(dimStyle.Render(fmt.Sprintf("  (%d-%d of %d)",
			sec.offset+1,
			min(sec.offset+sectionMaxVisible, len(sec.lines)),
			len(sec.lines))))
	}
	b.WriteString("\n")

	// Separator
	if focused {
		b.WriteString(" " + tableBorderStyle.Render(strings.Repeat("\u2500", 50)) + "\n")
	} else {
		b.WriteString(" " + dimStyle.Render(strings.Repeat("\u2500", 50)) + "\n")
	}

	// Up indicator
	if sec.needsScroll() && sec.offset > 0 {
		b.WriteString(dimStyle.Render("   \u25b2 more above") + "\n")
	}

	// Visible content
	hasCursor := len(sec.keys) > 0
	visibleStart := sec.offset
	for i, line := range sec.visibleLines() {
		absIdx := visibleStart + i
		if hasCursor && focused && absIdx == sec.itemCursor {
			b.WriteString(cursorStyle.Render(">") + line + "\n")
		} else {
			b.WriteString(" " + line + "\n")
		}
	}

	// Down indicator
	if sec.needsScroll() && sec.offset+sectionMaxVisible < len(sec.lines) {
		b.WriteString(dimStyle.Render("   \u25bc more below") + "\n")
	}

	b.WriteString("\n")
}

// fitToHeight ensures the rendered output is exactly height lines tall.
// It truncates excess content from the bottom (keeping the header visible)
// and pads with empty lines to fill the screen (preventing alt-screen artifacts).
func fitToHeight(content string, height int) string {
	if height <= 0 {
		return content
	}
	lines := strings.Split(content, "\n")
	// strings.Split on trailing \n produces an extra empty element; trim it
	// so we count only visual lines.
	if len(lines) > 0 && lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	if len(lines) > height {
		lines = lines[:height]
	}
	for len(lines) < height {
		lines = append(lines, "")
	}
	return strings.Join(lines, "\n")
}

// buildFilterHelp builds the standard help bar for views with sections and filtering.
func buildFilterHelp(sections []detailSection, filter filterState, extra ...string) string {
	parts := []string{"\u2191\u2193 Scroll"}
	if len(sections) > 1 {
		parts = append(parts, "Tab: Next section")
	}
	parts = append(parts, extra...)
	parts = append(parts, "/: Filter")
	if filter.input.text != "" {
		parts = append(parts, "Esc: Clear filter")
	} else {
		parts = append(parts, "Esc: Back")
	}
	return helpStyle.Render(" " + strings.Join(parts, "   "))
}

// textInput is a shared single-line text input with cursor movement support.
type textInput struct {
	text   string
	cursor int
}

func (t *textInput) handleKey(msg tea.KeyMsg) {
	switch msg.String() {
	case "left":
		if t.cursor > 0 {
			t.cursor--
		}
	case "right":
		if t.cursor < len(t.text) {
			t.cursor++
		}
	case "home", "ctrl+a":
		t.cursor = 0
	case "end", "ctrl+e":
		t.cursor = len(t.text)
	case "backspace":
		if t.cursor > 0 {
			t.text = t.text[:t.cursor-1] + t.text[t.cursor:]
			t.cursor--
		}
	case "delete":
		if t.cursor < len(t.text) {
			t.text = t.text[:t.cursor] + t.text[t.cursor+1:]
		}
	case "ctrl+u":
		t.text = ""
		t.cursor = 0
	default:
		if msg.Type == tea.KeyRunes {
			s := string(msg.Runes)
			t.text = t.text[:t.cursor] + s + t.text[t.cursor:]
			t.cursor += len(s)
		}
	}
}

func (t *textInput) clear() {
	t.text = ""
	t.cursor = 0
}

func (t *textInput) render() string {
	return t.text[:t.cursor] + "\u2588" + t.text[t.cursor:]
}

// filterState manages text filtering shared by multiple views.
type filterState struct {
	active bool
	input  textInput
}

func (f *filterState) startFiltering() {
	f.active = true
	f.input.clear()
}

func (f *filterState) matches(name string) bool {
	if f.input.text == "" {
		return true
	}
	return strings.Contains(strings.ToUpper(name), strings.ToUpper(f.input.text))
}

// handleFilterKey processes a key press while in filter mode.
// Returns true if sections need rebuilding.
func (f *filterState) handleFilterKey(msg tea.KeyMsg) bool {
	switch msg.String() {
	case "enter":
		f.active = false
		return true
	case "esc":
		f.active = false
		f.input.clear()
		return true
	default:
		old := f.input.text
		f.input.handleKey(msg)
		return f.input.text != old
	}
}

// handleEscOrQ handles esc/q when not in filter mode.
// Returns true if the filter was cleared (sections need rebuilding).
// Returns false if navigation back should happen.
func (f *filterState) handleEscOrQ() bool {
	if f.input.text != "" {
		f.input.clear()
		return true
	}
	return false
}

// renderFilterBar renders the filter input or active filter indicator.
func (f *filterState) renderFilterBar(b *strings.Builder) {
	if f.active {
		b.WriteString(fmt.Sprintf(" %s %s\n\n",
			cursorStyle.Render("/"),
			f.input.render()))
	} else if f.input.text != "" {
		b.WriteString(fmt.Sprintf(" %s %s\n\n",
			dimStyle.Render("Filter:"),
			connectedStyle.Render(f.input.text)))
	}
}
