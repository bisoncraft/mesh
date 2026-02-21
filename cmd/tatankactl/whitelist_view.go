package main

import (
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/bisoncraft/mesh/tatanka/admin"
	tea "github.com/charmbracelet/bubbletea"
)

const (
	wlModeNormal  = 0
	wlModeConfirm = 1
	wlModeInfo    = 2
)

type whitelistViewModel struct {
	state          *admin.AdminState
	api            *apiClient
	mode           int
	sections       []detailSection
	focusedSection int
	confirmAction  string
	confirmCmd     tea.Cmd
	height         int
	lastUpdate     time.Time
	statusMsg      string
}

func newWhitelistViewModel(state *admin.AdminState, api *apiClient) whitelistViewModel {
	m := whitelistViewModel{
		state: state,
		api:   api,
	}
	m.buildSections()
	return m
}

func (m *whitelistViewModel) buildSections() {
	m.sections = nil
	m.statusMsg = ""

	ourWl := getOurWhitelist(m.state)
	proposedWl := getOurProposal(m.state)
	hasProposal := len(proposedWl) > 0

	// Compute network proposals once for both sections.
	netProposals := computeNetworkProposals(m.state)

	// Build our proposed set once for matching against network proposals.
	ourProposedSet := make(map[string]bool, len(proposedWl))
	for _, id := range proposedWl {
		ourProposedSet[id] = true
	}

	var ourSupporters map[string]bool
	var otherProposals []networkProposal
	for _, np := range netProposals {
		if hasProposal && len(np.proposedPeerIDs) == len(proposedWl) && matchesSet(np.proposedPeerIDs, ourProposedSet) {
			ourSupporters = np.supporters
		} else {
			otherProposals = append(otherProposals, np)
		}
	}

	// Section 1: Our Whitelist
	title := "Our Whitelist"
	if hasProposal {
		title = "Our Whitelist " + mismatchStyle.Render("updating")
	}
	sec := detailSection{title: title}
	if m.state != nil {
		ourSet := make(map[string]bool, len(ourWl))
		for _, id := range ourWl {
			ourSet[id] = true
		}

		if hasProposal {
			proposedSet := make(map[string]bool, len(proposedWl))
			for _, id := range proposedWl {
				proposedSet[id] = true
			}

			// Sort: remaining first, then added, then removed.
			var remaining, added, removed []string
			for id := range ourSet {
				if proposedSet[id] {
					remaining = append(remaining, id)
				} else {
					removed = append(removed, id)
				}
			}
			for id := range proposedSet {
				if !ourSet[id] {
					added = append(added, id)
				}
			}
			sort.Strings(remaining)
			sort.Strings(added)
			sort.Strings(removed)

			for _, id := range remaining {
				short := truncatePeerID(id)
				connIcon := m.connectionIcon(id)
				sec.lines = append(sec.lines, fmt.Sprintf("   %s   %s", connIcon, short))
			}
			for _, id := range added {
				short := truncatePeerID(id)
				connIcon := m.connectionIcon(id)
				sec.lines = append(sec.lines, fmt.Sprintf("   %s %s %s", connIcon, diffGreenStyle.Render("+"), diffAddedBgStyle.Render(short)))
			}
			for _, id := range removed {
				short := truncatePeerID(id)
				connIcon := m.connectionIcon(id)
				sec.lines = append(sec.lines, fmt.Sprintf("   %s %s %s", connIcon, diffRedStyle.Render("-"), diffRemovedBgStyle.Render(short)))
			}

			// Consensus info for our proposal.
			if ourSupporters != nil {
				sec.lines = append(sec.lines, "")
				ci := computeConsensusInfo(m.state, ourWl, proposedWl, ourSupporters)
				sec.lines = append(sec.lines, m.renderConsensusLines(ci)...)
			}
		} else {
			for _, id := range ourWl {
				short := truncatePeerID(id)
				connIcon := m.connectionIcon(id)
				sec.lines = append(sec.lines, fmt.Sprintf("   %s %s", connIcon, short))
			}
		}

		if len(sec.lines) == 0 {
			sec.lines = append(sec.lines, dimStyle.Render("   No peers in whitelist"))
		}
	} else {
		sec.lines = append(sec.lines, dimStyle.Render("   Waiting for data..."))
	}
	m.sections = append(m.sections, sec)

	// Section 2: Network Proposals — only shown when there are proposals
	// from other peers (i.e., not just our own proposal).
	if len(otherProposals) > 0 {
		propSec := detailSection{title: "Network Proposals"}
		for i, np := range otherProposals {
			if i > 0 {
				propSec.lines = append(propSec.lines, "")
			}

			label := fmt.Sprintf("   Proposal %d", i+1)
			propSec.lines = append(propSec.lines, headerStyle.Render(label))

			// Align the key with the header line.
			for len(propSec.keys) < len(propSec.lines)-1 {
				propSec.keys = append(propSec.keys, "")
			}
			propSec.keys = append(propSec.keys, fmt.Sprintf("proposal:%d", i))

			// Show peers
			proposedSorted := make([]string, len(np.proposedPeerIDs))
			copy(proposedSorted, np.proposedPeerIDs)
			sort.Strings(proposedSorted)

			// Diff against current if possible
			if len(ourWl) > 0 {
				currentSet := make(map[string]bool, len(ourWl))
				for _, id := range ourWl {
					currentSet[id] = true
				}
				proposedSet := make(map[string]bool, len(proposedSorted))
				for _, id := range proposedSorted {
					proposedSet[id] = true
				}

				var added, removed []string
				for _, id := range proposedSorted {
					if !currentSet[id] {
						added = append(added, truncatePeerID(id))
					}
				}
				for _, id := range ourWl {
					if !proposedSet[id] {
						removed = append(removed, truncatePeerID(id))
					}
				}

				if len(added) > 0 || len(removed) > 0 {
					var changes []string
					for _, id := range added {
						changes = append(changes, diffGreenStyle.Render("+"+id))
					}
					for _, id := range removed {
						changes = append(changes, diffRedStyle.Render("-"+id))
					}
					propSec.lines = append(propSec.lines, "   "+strings.Join(changes, ", "))
				} else {
					propSec.lines = append(propSec.lines, dimStyle.Render("   (same as current whitelist)"))
				}
			} else {
				for _, id := range proposedSorted {
					propSec.lines = append(propSec.lines, fmt.Sprintf("     %s", truncatePeerID(id)))
				}
			}

			// Consensus display
			ci := computeConsensusInfo(m.state, ourWl, np.proposedPeerIDs, np.supporters)
			propSec.lines = append(propSec.lines, m.renderConsensusLines(ci)...)
		}
		// Pad keys to match final lines length.
		for len(propSec.keys) < len(propSec.lines) {
			propSec.keys = append(propSec.keys, "")
		}
		m.sections = append(m.sections, propSec)
	}

	if m.focusedSection >= len(m.sections) {
		m.focusedSection = 0
	}
}

func (m *whitelistViewModel) connectionIcon(peerID string) string {
	if m.state == nil {
		return dimStyle.Render("\u25cf")
	}
	if peerID == m.state.OurPeerID {
		return getStateIcon(admin.StateConnected)
	}
	peer, ok := m.state.Peers[peerID]
	if !ok {
		return dimStyle.Render("\u25cf")
	}
	return getStateIcon(peer.State)
}

// matchesSet returns true if every element in ids is in the set.
func matchesSet(ids []string, set map[string]bool) bool {
	for _, id := range ids {
		if !set[id] {
			return false
		}
	}
	return true
}

// renderConsensusLines renders the consensus progress display for a proposal.
func (m *whitelistViewModel) renderConsensusLines(ci consensusInfo) []string {
	var lines []string

	header := fmt.Sprintf("   Consensus (%d of %d overlap agreeing, need %d)", ci.agreeing, ci.nOverlap, ci.threshold)
	if ci.agreeing >= ci.threshold && ci.blocking == 0 {
		header += " " + connectedStyle.Render("\u2713")
	}
	lines = append(lines, header)

	if ci.blocking > 0 {
		plural := ""
		if ci.blocking > 1 {
			plural = "s"
		}
		lines = append(lines, disconnectedStyle.Render(fmt.Sprintf("   \u26a0 %d online peer%s not agreeing \u2014 update blocked", ci.blocking, plural)))
	}

	for _, op := range ci.overlap {
		connIcon := m.connectionIcon(op.peerID)
		short := truncatePeerID(op.peerID)
		var status string
		if op.agreeing {
			if op.ready {
				status = connectedStyle.Render("agreeing (ready)")
			} else {
				status = connectedStyle.Render("agreeing")
			}
		} else if op.online {
			status = disconnectedStyle.Render("not agreeing")
		} else {
			status = dimStyle.Render("offline")
		}
		lines = append(lines, fmt.Sprintf("     %s %s  %s", connIcon, short, status))
	}

	return lines
}

func (m whitelistViewModel) Update(msg tea.Msg) (whitelistViewModel, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.height = msg.Height
	case tea.KeyMsg:
		if m.mode == wlModeInfo {
			switch msg.String() {
			case "esc", "q", "i":
				m.mode = wlModeNormal
			}
			return m, nil
		}

		if m.mode == wlModeConfirm {
			switch msg.String() {
			case "y":
				m.mode = wlModeNormal
				return m, m.confirmCmd
			case "n", "esc":
				m.mode = wlModeNormal
				m.confirmAction = ""
				m.confirmCmd = nil
			}
			return m, nil
		}

		// Normal mode
		switch msg.String() {
		case "up", "k":
			if len(m.sections) > 0 {
				sec := &m.sections[m.focusedSection]
				if len(sec.keys) > 0 {
					sec.cursorUp()
				} else {
					sec.scrollUp()
				}
			}
		case "down", "j":
			if len(m.sections) > 0 {
				sec := &m.sections[m.focusedSection]
				if len(sec.keys) > 0 {
					sec.cursorDown()
				} else {
					sec.scrollDown()
				}
			}
		case "tab":
			if len(m.sections) > 1 {
				m.focusedSection = (m.focusedSection + 1) % len(m.sections)
			}
		case "shift+tab":
			if len(m.sections) > 1 {
				m.focusedSection = (m.focusedSection - 1 + len(m.sections)) % len(m.sections)
			}
		case "enter":
			// Adopt a network proposal — only when proposals section exists and is focused.
			if m.focusedSection > 0 && m.focusedSection < len(m.sections) {
				proposalIdx := m.findProposalAtCursor()
				if proposalIdx >= 0 {
					ourProposal := getOurProposal(m.state)
					ourSet := make(map[string]bool, len(ourProposal))
					for _, id := range ourProposal {
						ourSet[id] = true
					}
					var others []networkProposal
					for _, np := range computeNetworkProposals(m.state) {
						if len(np.proposedPeerIDs) != len(ourProposal) || !matchesSet(np.proposedPeerIDs, ourSet) {
							others = append(others, np)
						}
					}
					if proposalIdx < len(others) {
						np := others[proposalIdx]
						// Cannot adopt a whitelist that removes us.
						if m.state != nil && !slices.Contains(np.proposedPeerIDs, m.state.OurPeerID) {
							m.statusMsg = "Cannot adopt: proposal does not include our peer"
							return m, nil
						}
						m.mode = wlModeConfirm
						m.confirmAction = fmt.Sprintf("Propose whitelist with %d peers?", len(np.proposedPeerIDs))
						m.confirmCmd = m.api.proposeWhitelist(np.proposedPeerIDs)
					}
				}
			}
		case "i":
			m.mode = wlModeInfo
			return m, nil
		case "p":
			// Open whitelist editor
			return m, func() tea.Msg {
				return navigateToWhitelistEditorMsg{}
			}
		case "c":
			// Clear our active proposal
			if len(getOurProposal(m.state)) > 0 {
				m.mode = wlModeConfirm
				m.confirmAction = "Clear active proposal?"
				m.confirmCmd = m.api.clearProposal()
			}
		case "esc", "q":
			return m, navBack()
		}
	}
	return m, nil
}

func (m *whitelistViewModel) findProposalAtCursor() int {
	if m.focusedSection <= 0 || m.focusedSection >= len(m.sections) {
		return -1
	}
	sec := &m.sections[m.focusedSection]
	if len(sec.keys) == 0 {
		return -1
	}
	key := sec.selectedKey()
	if key == "" {
		return -1
	}
	var idx int
	if _, err := fmt.Sscanf(key, "proposal:%d", &idx); err == nil {
		return idx
	}
	return -1
}

func (m whitelistViewModel) View() string {
	var b strings.Builder

	// Header
	ts := ""
	if !m.lastUpdate.IsZero() {
		ts = dimStyle.Render(m.lastUpdate.Format("15:04:05"))
	}
	b.WriteString(fmt.Sprintf(" %s%s\n\n",
		headerStyle.Render("Whitelist Management"),
		pad(ts, 40)))

	// Info overlay
	if m.mode == wlModeInfo {
		b.WriteString(dimStyle.Render(" A whitelist update is a two-phase process. First, a node proposes a new") + "\n")
		b.WriteString(dimStyle.Render(" whitelist and publishes it to the network. Other nodes that agree adopt the") + "\n")
		b.WriteString(dimStyle.Render(" same proposal. Once 2/3 of the overlapping peers (peers in both the current") + "\n")
		b.WriteString(dimStyle.Render(" and proposed whitelists) are online and agreeing, all participating nodes mark") + "\n")
		b.WriteString(dimStyle.Render(" themselves as ready. If any overlapping peer is online but has not adopted the") + "\n")
		b.WriteString(dimStyle.Render(" proposal or supports a different proposal, the update will not proceed. When") + "\n")
		b.WriteString(dimStyle.Render(" all online agreeing nodes are ready, the switch executes automatically.") + "\n")
		b.WriteString(helpStyle.Render("\n Esc: Back"))
		return fitToHeight(b.String(), m.height)
	}

	if m.state == nil {
		b.WriteString(" Waiting for data...\n")
		b.WriteString(helpStyle.Render("\n Esc: Back"))
		return fitToHeight(b.String(), m.height)
	}

	// Status message
	if m.statusMsg != "" {
		b.WriteString(fmt.Sprintf(" %s\n\n", connectedStyle.Render(m.statusMsg)))
	}

	// Confirm mode overlay
	if m.mode == wlModeConfirm {
		b.WriteString(fmt.Sprintf(" %s %s\n\n",
			cursorStyle.Render(m.confirmAction),
			dimStyle.Render("(y/n)")))
	}

	// Sections
	for i := range m.sections {
		renderSection(&b, &m.sections[i], i == m.focusedSection)
	}

	// Help
	var parts []string
	parts = append(parts, "\u2191\u2193 Scroll")
	if len(m.sections) > 1 {
		parts = append(parts, "Tab: Section", "Enter: Adopt")
	}
	parts = append(parts, "p: Propose")
	if len(getOurProposal(m.state)) > 0 {
		parts = append(parts, "c: Clear proposal")
	}
	parts = append(parts, "i: Info", "Esc: Back")
	b.WriteString(helpStyle.Render(" " + strings.Join(parts, "   ")))

	return fitToHeight(b.String(), m.height)
}
