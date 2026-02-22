package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/apigatewayv2"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type panelID int

type panelLevel int

type mode int

const (
	leftPanel panelID = iota
	rightPanel
)

const (
	levelProfiles panelLevel = iota
	levelRegions
	levelServices
	levelResources
)

const (
	modeNormal mode = iota
	modeInput
	modeView
	modeConfirm
	modeConflict
	modeLambdaConflict
	modeGrantRegistry
)

type resourceItem struct {
	Label string
	Value string
	Kind  string // profile|region|service|bucket|prefix|object|back|generic
}

type panelState struct {
	Level     panelLevel
	Profile   string
	Region    string
	Service   string
	S3Bucket  string
	S3Prefix  string
	Resources []resourceItem
	Marked    map[string]bool
	Cursor    int
	Loading   bool
	Err       string
}

type inputState struct {
	Title   string
	Prompt  string
	Target  string
	PanelID panelID
	Extra   string
	Input   textinput.Model
}

type confirmState struct {
	Title   string
	Target  string
	PanelID panelID
	Payload string
}

type conflictState struct {
	Title        string
	Src          panelState
	Dst          panelState
	Keys         []string
	Move         bool
	ExistingKeys map[string]bool
}

type lambdaConflictState struct {
	Title        string
	Src          panelState
	Dst          panelState
	Names        []string
	Existing     map[string]bool
	SkipExisting bool
}

type grantRegistryState struct {
	Items  []policyGrantRecord
	Cursor int
}

type model struct {
	width              int
	height             int
	active             panelID
	mode               mode
	status             string
	lastInfo           string
	logLines           []string
	left               panelState
	right              panelState
	input              inputState
	confirm            confirmState
	conflict           conflictState
	lconf              lambdaConflictState
	grants             grantRegistryState
	viewText           string
	copyJob            *copyJobState
	lambdaJob          *lambdaCopyJobState
	spaceHoldStart     time.Time
	spaceLastEvent     time.Time
	spaceHoldTriggered bool
}

type loadedPanelMsg struct {
	PanelID panelID
	Items   []resourceItem
	Err     error
	Action  string
}

type detailMsg struct {
	Text   string
	Err    error
	Action string
}

type opMsg struct {
	Status string
	Err    error
	Action string
}

type copyJobState struct {
	Src     panelState
	Dst     panelState
	Keys    []string
	Move    bool
	Index   int
	Success int
	Failed  int
	DstCli  *s3.Client
	SrcCli  *s3.Client
}

type copyProgressMsg struct {
	Job    copyJobState
	Key    string
	Err    error
	Action string
}

type lambdaCopyJobState struct {
	Src              panelState
	Dst              panelState
	Names            []string
	Index            int
	Success          int
	Failed           int
	Skipped          int
	SkipExisting     bool
	Existing         map[string]bool
	AutoGrantedRoles map[string]bool
	DstPrincipalArn  string
	SrcCli           *lambda.Client
	DstCli           *lambda.Client
	DstIamCli        *iam.Client
}

type lambdaCopyProgressMsg struct {
	Job     lambdaCopyJobState
	Name    string
	Err     error
	Skipped bool
	Action  string
}

type bucketCopyPreparedMsg struct {
	SrcBucket string
	Dst       panelState
	Move      bool
	Keys      []string
	Err       error
	Action    string
}

type copyPreflightMsg struct {
	Src          panelState
	Dst          panelState
	Keys         []string
	Move         bool
	ExistingKeys map[string]bool
	Err          error
	Action       string
}

type lambdaCopyPreflightMsg struct {
	Src      panelState
	Dst      panelState
	Names    []string
	Existing map[string]bool
	Err      error
	Action   string
}

type grantRevokeMsg struct {
	Err     error
	Status  string
	GrantID string
}

var services = []string{"s3", "lambda", "vpc", "apigateway"}
var defaultRegions = []string{"us-east-1", "us-east-2", "us-west-1", "us-west-2", "eu-west-1", "eu-central-1", "sa-east-1"}
var profileAccountCache = struct {
	sync.RWMutex
	values map[string]string
}{values: map[string]string{}}

const sessionFileName = ".nc-aws-session.json"
const policyRegistryFileName = ".nc-aws-policy-registry.json"

type persistedPanelState struct {
	Level    panelLevel `json:"level"`
	Profile  string     `json:"profile"`
	Region   string     `json:"region"`
	Service  string     `json:"service"`
	S3Bucket string     `json:"s3_bucket"`
	S3Prefix string     `json:"s3_prefix"`
	Cursor   int        `json:"cursor"`
}

type persistedSessionState struct {
	Active panelID             `json:"active"`
	Left   persistedPanelState `json:"left"`
	Right  persistedPanelState `json:"right"`
}

type policyGrantRecord struct {
	GrantType                   string   `json:"grant_type,omitempty"` // s3|lambda
	ID                          string   `json:"id"`
	CreatedAt                   string   `json:"created_at"`
	SourceProfile               string   `json:"source_profile"`
	SourceRegion                string   `json:"source_region"`
	SourceBucket                string   `json:"source_bucket"`
	DestinationProfile          string   `json:"destination_profile"`
	DestinationRegion           string   `json:"destination_region"`
	DestinationPrincipalARN     string   `json:"destination_principal_arn"`
	SidGetObject                string   `json:"sid_get_object"`
	SidListBucket               string   `json:"sid_list_bucket"`
	IamPolicyArn                string   `json:"iam_policy_arn,omitempty"`
	IamPolicyName               string   `json:"iam_policy_name,omitempty"`
	LambdaRoleArn               string   `json:"lambda_role_arn,omitempty"`
	LambdaRoleCreated           bool     `json:"lambda_role_created,omitempty"`
	LambdaRoleManagedPolicyArns []string `json:"lambda_role_managed_policy_arns,omitempty"`
	Revoked                     bool     `json:"revoked"`
	RevokedAt                   string   `json:"revoked_at,omitempty"`
}

type policyRegistry struct {
	Records []policyGrantRecord `json:"records"`
}

func main() {
	m := model{
		active:   leftPanel,
		mode:     modeNormal,
		status:   "Loading profiles...",
		lastInfo: "Loading profiles...",
		logLines: []string{"boot: loading profiles..."},
		left:     panelState{Level: levelProfiles, Marked: map[string]bool{}, Loading: true},
		right:    panelState{Level: levelProfiles, Marked: map[string]bool{}, Loading: true},
	}
	if saved, err := loadSessionState(); err == nil {
		applyPersistedSession(&m, saved)
		m.status = "Session restored"
		m.lastInfo = "Session restored"
		m.logLines = append(m.logLines, "session: restored from disk")
	}

	p := tea.NewProgram(m, tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

func (m model) Init() tea.Cmd {
	return tea.Batch(loadPanelCmd(leftPanel, m.left), loadPanelCmd(rightPanel, m.right))
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		return m, nil
	case loadedPanelMsg:
		p := m.getPanel(msg.PanelID)
		p.Loading = false
		if msg.Err != nil {
			p.Err = msg.Err.Error()
			p.Resources = nil
			p.Cursor = 0
			m.status = "Load error: " + firstLine(msg.Err.Error())
			m.lastInfo = msg.Err.Error()
			m.pushLog(fmt.Sprintf("ERROR %s :: %s", msg.Action, msg.Err.Error()))
		} else {
			p.Err = ""
			p.Resources = msg.Items
			p.Marked = keepOnlyVisibleMarks(p.Marked, p.Resources)
			if p.Cursor >= len(p.Resources) {
				p.Cursor = max(0, len(p.Resources)-1)
			}
			m.status = fmt.Sprintf("Loaded %d items", len(msg.Items))
			m.lastInfo = m.status
			m.pushLog(fmt.Sprintf("OK %s :: %d item(s)", msg.Action, len(msg.Items)))
		}
		m.setPanel(msg.PanelID, p)
		m.persistContext()
		return m, nil
	case detailMsg:
		if msg.Err != nil {
			m.status = "Detail error: " + firstLine(msg.Err.Error())
			m.lastInfo = msg.Err.Error()
			m.pushLog(fmt.Sprintf("ERROR %s :: %s", msg.Action, msg.Err.Error()))
			m.mode = modeNormal
			return m, nil
		}
		m.viewText = msg.Text
		m.pushLog("OK " + msg.Action)
		m.mode = modeView
		return m, nil
	case opMsg:
		if msg.Err != nil {
			help := suggestFixForError(msg.Action, msg.Err)
			m.status = "Operation error: " + firstLine(msg.Err.Error())
			if help != "" {
				m.status = firstLine(help)
				m.lastInfo = msg.Err.Error() + "\n\nSuggested fix:\n" + help
			} else {
				m.lastInfo = msg.Err.Error()
			}
			m.pushLog(fmt.Sprintf("ERROR %s :: %s", msg.Action, msg.Err.Error()))
			if help != "" {
				m.pushLog("HINT :: " + help)
			}
		} else {
			m.status = msg.Status
			m.lastInfo = msg.Status
			m.pushLog("OK " + msg.Action + " :: " + msg.Status)
		}
		m.mode = modeNormal
		m.persistContext()
		return m, tea.Batch(loadPanelCmd(leftPanel, m.left), loadPanelCmd(rightPanel, m.right))
	case copyProgressMsg:
		job := msg.Job
		if msg.Err != nil {
			job.Failed++
			m.pushLog(fmt.Sprintf("ERROR %s :: key=%s :: %s", msg.Action, msg.Key, msg.Err.Error()))
		} else {
			job.Success++
			m.pushLog(fmt.Sprintf("OK %s :: key=%s", msg.Action, msg.Key))
		}
		job.Index++
		m.copyJob = &job
		m.status = fmt.Sprintf("Copy progress: %d/%d (ok=%d fail=%d)", job.Index, len(job.Keys), job.Success, job.Failed)

		if job.Index >= len(job.Keys) {
			if job.Failed > 0 {
				m.status = fmt.Sprintf("Copy finished with errors: ok=%d fail=%d", job.Success, job.Failed)
				m.lastInfo = m.status
			} else {
				m.status = fmt.Sprintf("Copy completed: %d object(s)", job.Success)
				m.lastInfo = m.status
			}
			m.copyJob = nil
			m.mode = modeNormal
			m.persistContext()
			return m, tea.Batch(loadPanelCmd(leftPanel, m.left), loadPanelCmd(rightPanel, m.right))
		}

		nextKey := shortKey(job.Keys[job.Index], 72)
		if job.Move {
			m.status = fmt.Sprintf("Moving %d/%d :: %s", job.Index+1, len(job.Keys), nextKey)
		} else {
			m.status = fmt.Sprintf("Copying %d/%d :: %s", job.Index+1, len(job.Keys), nextKey)
		}
		return m, copyStepCmd(job)
	case bucketCopyPreparedMsg:
		if msg.Err != nil {
			m.status = "Operation error: " + firstLine(msg.Err.Error())
			m.lastInfo = msg.Err.Error()
			m.pushLog(fmt.Sprintf("ERROR %s :: %s", msg.Action, msg.Err.Error()))
			return m, nil
		}
		if len(msg.Keys) == 0 {
			m.status = "No objects found in source bucket"
			m.lastInfo = m.status
			m.pushLog("OK " + msg.Action + " :: 0 object(s)")
			return m, nil
		}
		src := m.activePanel()
		src.S3Bucket = msg.SrcBucket
		m.status = "Checking existing objects in destination..."
		m.pushLog(fmt.Sprintf("START check copy conflicts %s/%s -> %s/%s (%d)", src.Profile, src.S3Bucket, msg.Dst.Profile, msg.Dst.S3Bucket, len(msg.Keys)))
		return m, preflightCopyCmd(src, msg.Dst, msg.Keys, msg.Move)
	case copyPreflightMsg:
		if msg.Err != nil {
			m.status = "Preflight error: " + firstLine(msg.Err.Error())
			m.lastInfo = msg.Err.Error()
			m.pushLog(fmt.Sprintf("ERROR %s :: %s", msg.Action, msg.Err.Error()))
			return m, nil
		}
		conflicts := len(msg.ExistingKeys)
		m.pushLog(fmt.Sprintf("OK %s :: %d conflict(s)", msg.Action, conflicts))
		if conflicts == 0 {
			return m, m.startCopyJob(msg.Src, msg.Dst, msg.Keys, msg.Move)
		}
		modeLabel := "copy"
		if msg.Move {
			modeLabel = "move"
		}
		m.conflict = conflictState{
			Title:        fmt.Sprintf("Found %d existing object(s) in destination for %s. [o] overwrite all  [s] skip existing  [esc] cancel", conflicts, modeLabel),
			Src:          msg.Src,
			Dst:          msg.Dst,
			Keys:         msg.Keys,
			Move:         msg.Move,
			ExistingKeys: msg.ExistingKeys,
		}
		m.mode = modeConflict
		m.status = fmt.Sprintf("Conflicts detected: %d existing object(s)", conflicts)
		return m, nil
	case lambdaCopyPreflightMsg:
		if msg.Err != nil {
			m.status = "Preflight error: " + firstLine(msg.Err.Error())
			m.lastInfo = msg.Err.Error()
			m.pushLog(fmt.Sprintf("ERROR %s :: %s", msg.Action, msg.Err.Error()))
			return m, nil
		}
		conflicts := len(msg.Existing)
		m.pushLog(fmt.Sprintf("OK %s :: %d conflict(s)", msg.Action, conflicts))
		if conflicts == 0 {
			return m, m.startLambdaCopyJob(msg.Src, msg.Dst, msg.Names, false, msg.Existing)
		}
		m.lconf = lambdaConflictState{
			Title:    fmt.Sprintf("Found %d existing Lambda(s) in destination. [o] overwrite all  [s] skip existing  [esc] cancel", conflicts),
			Src:      msg.Src,
			Dst:      msg.Dst,
			Names:    msg.Names,
			Existing: msg.Existing,
		}
		m.mode = modeLambdaConflict
		m.status = fmt.Sprintf("Conflicts detected: %d existing Lambda(s)", conflicts)
		return m, nil
	case lambdaCopyProgressMsg:
		job := msg.Job
		if msg.Err != nil {
			job.Failed++
			m.pushLog(fmt.Sprintf("ERROR %s :: lambda=%s :: %s", msg.Action, msg.Name, msg.Err.Error()))
		} else if msg.Skipped {
			job.Skipped++
			m.pushLog(fmt.Sprintf("SKIP %s :: lambda=%s", msg.Action, msg.Name))
		} else {
			job.Success++
			m.pushLog(fmt.Sprintf("OK %s :: lambda=%s", msg.Action, msg.Name))
		}
		job.Index++
		m.lambdaJob = &job
		m.status = fmt.Sprintf("Lambda copy progress: %d/%d (ok=%d skip=%d fail=%d)", job.Index, len(job.Names), job.Success, job.Skipped, job.Failed)
		if job.Index >= len(job.Names) {
			if job.Failed > 0 {
				m.status = fmt.Sprintf("Lambda copy finished with errors: ok=%d skip=%d fail=%d", job.Success, job.Skipped, job.Failed)
			} else {
				m.status = fmt.Sprintf("Lambda copy completed: ok=%d skip=%d", job.Success, job.Skipped)
			}
			m.lastInfo = m.status
			m.lambdaJob = nil
			m.mode = modeNormal
			m.persistContext()
			return m, tea.Batch(loadPanelCmd(leftPanel, m.left), loadPanelCmd(rightPanel, m.right))
		}
		next := shortKey(job.Names[job.Index], 72)
		m.status = fmt.Sprintf("Copying lambda %d/%d :: %s", job.Index+1, len(job.Names), next)
		return m, lambdaCopyStepCmd(job)
	case grantRevokeMsg:
		if msg.Err != nil {
			m.status = "Revoke error: " + firstLine(msg.Err.Error())
			m.lastInfo = msg.Err.Error()
			m.pushLog(fmt.Sprintf("ERROR revoke grant %s :: %s", msg.GrantID, msg.Err.Error()))
		} else {
			m.status = msg.Status
			m.lastInfo = msg.Status
			m.pushLog("OK revoke grant " + msg.GrantID + " :: " + msg.Status)
		}
		m.reloadGrantRegistry()
		m.mode = modeGrantRegistry
		return m, nil
	case tea.KeyMsg:
		if m.lambdaJob != nil {
			switch msg.String() {
			case "q", "ctrl+c":
				return m, tea.Quit
			case "l":
				m.viewText = strings.Join(m.logLines, "\n")
				if strings.TrimSpace(m.viewText) == "" {
					m.viewText = "(no logs)"
				}
				m.mode = modeView
			}
			return m, nil
		}
		if m.copyJob != nil {
			// Keep UI responsive but prevent launching conflicting operations while copy runs.
			switch msg.String() {
			case "q", "ctrl+c":
				return m, tea.Quit
			case "l":
				m.viewText = strings.Join(m.logLines, "\n")
				if strings.TrimSpace(m.viewText) == "" {
					m.viewText = "(no logs)"
				}
				m.mode = modeView
			}
			return m, nil
		}

		switch m.mode {
		case modeInput:
			return m.updateInput(msg)
		case modeView:
			if msg.String() == "esc" || msg.String() == "q" || msg.String() == "enter" {
				m.mode = modeNormal
			}
			return m, nil
		case modeConfirm:
			if msg.String() == "y" {
				m.active = m.confirm.PanelID
				m.persistContext()
				m.mode = modeNormal
				return m, m.runOp(m.confirm.Target, m.confirm.Payload, "")
			}
			if msg.String() == "n" || msg.String() == "esc" {
				m.mode = modeNormal
				m.status = "Canceled"
			}
			return m, nil
		case modeConflict:
			switch msg.String() {
			case "o":
				m.mode = modeNormal
				return m, m.startCopyJob(m.conflict.Src, m.conflict.Dst, m.conflict.Keys, m.conflict.Move)
			case "s":
				keys := make([]string, 0, len(m.conflict.Keys))
				for _, k := range m.conflict.Keys {
					if !m.conflict.ExistingKeys[k] {
						keys = append(keys, k)
					}
				}
				m.mode = modeNormal
				if len(keys) == 0 {
					m.status = "Nothing to copy: all selected objects already exist in destination"
					m.lastInfo = m.status
					m.pushLog("OK conflict resolution :: skipped all existing objects")
					return m, nil
				}
				m.pushLog(fmt.Sprintf("OK conflict resolution :: skip existing, %d object(s) remain", len(keys)))
				return m, m.startCopyJob(m.conflict.Src, m.conflict.Dst, keys, m.conflict.Move)
			case "esc", "q", "c":
				m.mode = modeNormal
				m.status = "Canceled"
				m.pushLog("OK conflict resolution :: canceled")
				return m, nil
			}
			return m, nil
		case modeLambdaConflict:
			switch msg.String() {
			case "o":
				m.mode = modeNormal
				return m, m.startLambdaCopyJob(m.lconf.Src, m.lconf.Dst, m.lconf.Names, false, m.lconf.Existing)
			case "s":
				m.mode = modeNormal
				return m, m.startLambdaCopyJob(m.lconf.Src, m.lconf.Dst, m.lconf.Names, true, m.lconf.Existing)
			case "esc", "q", "c":
				m.mode = modeNormal
				m.status = "Canceled"
				m.pushLog("OK lambda conflict resolution :: canceled")
				return m, nil
			}
			return m, nil
		case modeGrantRegistry:
			switch msg.String() {
			case "esc", "q", "a":
				m.mode = modeNormal
				m.status = "Closed grants registry"
				return m, nil
			case "j", "down":
				if m.grants.Cursor < len(m.grants.Items)-1 {
					m.grants.Cursor++
				}
				return m, nil
			case "k", "up":
				if m.grants.Cursor > 0 {
					m.grants.Cursor--
				}
				return m, nil
			case "u":
				if len(m.grants.Items) == 0 {
					m.status = "No active grants to revoke"
					return m, nil
				}
				rec := m.grants.Items[m.grants.Cursor]
				m.status = "Revoking selected grant..."
				return m, revokeGrantRecordCmd(rec)
			case "v", "enter":
				if len(m.grants.Items) == 0 {
					return m, nil
				}
				b, _ := json.MarshalIndent(m.grants.Items[m.grants.Cursor], "", "  ")
				m.viewText = string(b)
				m.mode = modeView
				return m, nil
			}
			return m, nil
		}

		switch msg.String() {
		case "ctrl+c", "q":
			return m, tea.Quit
		case "tab":
			m.active = opposite(m.active)
			m.persistContext()
			return m, nil
		case "i":
			p := m.activePanel()
			resetToStart(&p)
			m.setActivePanel(p)
			m.persistContext()
			return m, loadPanelCmd(m.active, p)
		case "j", "down":
			p := m.activePanel()
			if p.Cursor < len(p.Resources)-1 {
				p.Cursor++
			}
			m.setActivePanel(p)
			m.persistContext()
			return m, nil
		case "k", "up":
			p := m.activePanel()
			if p.Cursor > 0 {
				p.Cursor--
			}
			m.setActivePanel(p)
			m.persistContext()
			return m, nil
		case "g":
			return m, loadPanelCmd(m.active, m.activePanel())
		case "e":
			m.viewText = m.lastInfo
			if strings.TrimSpace(m.viewText) == "" {
				m.viewText = m.status
			}
			m.mode = modeView
			return m, nil
		case "l":
			m.viewText = strings.Join(m.logLines, "\n")
			if strings.TrimSpace(m.viewText) == "" {
				m.viewText = "(no logs)"
			}
			m.mode = modeView
			return m, nil
		case " ":
			now := time.Now()
			prevLast := m.spaceLastEvent
			firstPress := prevLast.IsZero() || now.Sub(prevLast) > 400*time.Millisecond
			// Reset hold tracking if this is a fresh press sequence.
			if firstPress {
				m.spaceHoldStart = now
				m.spaceHoldTriggered = false
			}
			m.spaceLastEvent = now

			p := m.activePanel()
			it, ok := selectedItem(p)
			if !ok || p.Level != levelResources {
				m.status = "Nothing to mark here"
				return m, nil
			}

			// Hold SPACE ~3s (auto-repeat) to mark all selectable items in the panel.
			if !m.spaceHoldTriggered && now.Sub(m.spaceHoldStart) >= 3*time.Second {
				count := markAllSelectableItems(&p)
				m.spaceHoldTriggered = true
				m.setActivePanel(p)
				m.persistContext()
				m.status = fmt.Sprintf("Selected %d item(s)", count)
				return m, nil
			}

			// Ignore auto-repeat events while key is held to avoid rapid toggle flicker.
			if !firstPress {
				return m, nil
			}

			markable := false
			if p.Service == "s3" {
				markable = it.Kind == "object" || it.Kind == "bucket"
			} else {
				markable = it.Kind == "generic"
			}
			if !markable {
				m.status = "This item cannot be marked"
				return m, nil
			}
			if p.Marked == nil {
				p.Marked = map[string]bool{}
			}
			if p.Marked[it.Value] {
				delete(p.Marked, it.Value)
			} else {
				p.Marked[it.Value] = true
			}
			m.setActivePanel(p)
			m.persistContext()
			return m, nil
		case "backspace", "h":
			p := m.activePanel()
			stepBack(&p)
			m.setActivePanel(p)
			m.persistContext()
			return m, loadPanelCmd(m.active, p)
		case "enter":
			p := m.activePanel()
			it, ok := selectedItem(p)
			if !ok {
				m.status = "No item selected"
				return m, nil
			}
			handled, nextLoad := m.handleEnter(&p, it)
			if handled {
				m.setActivePanel(p)
				m.persistContext()
				if nextLoad {
					return m, loadPanelCmd(m.active, p)
				}
				return m, nil
			}
			return m, detailCmd(p, it)
		case "v":
			p := m.activePanel()
			it, ok := selectedItem(p)
			if !ok {
				m.status = "No item selected"
				return m, nil
			}
			return m, detailCmd(p, it)
		case "n":
			p := m.activePanel()
			if !(p.Level == levelResources && p.Service == "s3" && p.S3Bucket == "") {
				m.status = "Create bucket only available in S3 bucket list"
				return m, nil
			}
			m.mode = modeInput
			m.input = newInputState("Create S3 Bucket", "Bucket name", "create_s3_bucket", m.active, "", "")
			return m, nil
		case "d":
			p := m.activePanel()
			it, ok := selectedItem(p)
			if !ok {
				m.status = "No item selected"
				return m, nil
			}
			if p.Level == levelResources && p.Service == "s3" && it.Kind == "object" {
				m.mode = modeConfirm
				m.confirm = confirmState{Title: "Delete S3 object? y/n", Target: "delete_s3_object", PanelID: m.active, Payload: it.Value}
				return m, nil
			}
			if p.Level == levelResources && p.Service == "s3" && it.Kind == "bucket" {
				m.mode = modeConfirm
				m.confirm = confirmState{Title: "Delete bucket and all objects? y/n", Target: "delete_s3_bucket_full", PanelID: m.active, Payload: it.Value}
				return m, nil
			}
			if p.Level == levelResources && p.Service == "lambda" {
				marked := selectedGenericValues(p)
				if len(marked) > 0 {
					m.mode = modeConfirm
					m.confirm = confirmState{
						Title:   fmt.Sprintf("Delete %d Lambda(s)? y/n", len(marked)),
						Target:  "delete_lambda_bulk",
						PanelID: m.active,
						Payload: strings.Join(marked, "\n"),
					}
					return m, nil
				}
				m.mode = modeConfirm
				m.confirm = confirmState{Title: "Delete Lambda? y/n", Target: "delete_lambda", PanelID: m.active, Payload: it.Value}
				return m, nil
			}
			m.status = "Delete not implemented here"
			return m, nil
		case "c":
			src := m.activePanel()
			dst := m.getPanel(opposite(m.active))
			if src.Level == levelResources && dst.Level == levelResources && src.Service == "s3" && dst.Service == "s3" {
				return m, m.startS3CopyOrMove(false)
			}
			if src.Level == levelResources && dst.Level == levelResources && src.Service == "lambda" && dst.Service == "lambda" {
				names := selectedGenericValues(src)
				if len(names) == 0 {
					it, ok := selectedItem(src)
					if ok && it.Kind == "generic" {
						names = []string{it.Value}
					}
				}
				if len(names) == 0 {
					m.status = "Select Lambda(s) to copy"
					return m, nil
				}
				m.status = "Checking existing Lambdas in destination..."
				m.pushLog(fmt.Sprintf("START check lambda copy conflicts %s/%s -> %s/%s (%d)", src.Profile, src.Region, dst.Profile, dst.Region, len(names)))
				return m, preflightLambdaCopyCmd(src, dst, names)
			}
			m.status = "Copy requires both panels in S3 or Lambda resource view"
			return m, nil
		case "m":
			src := m.activePanel()
			dst := m.getPanel(opposite(m.active))
			if !(src.Level == levelResources && dst.Level == levelResources && src.Service == "s3" && dst.Service == "s3") {
				m.status = "Move requires both panels in S3 resource view"
				return m, nil
			}
			return m, m.startS3CopyOrMove(true)
		case "a":
			m.reloadGrantRegistry()
			m.mode = modeGrantRegistry
			m.status = fmt.Sprintf("Loaded %d active grant(s)", len(m.grants.Items))
			return m, nil
		case "u":
			m.status = "Press 'a' to open grants registry, then 'u' to revoke selected grant"
			return m, nil
		}
	}

	return m, nil
}

func (m model) handleEnter(p *panelState, it resourceItem) (handled bool, nextLoad bool) {
	switch p.Level {
	case levelProfiles:
		if it.Kind == "profile" {
			p.Profile = resolveProfileCase(it.Value)
			p.Region = ""
			p.Service = ""
			p.S3Bucket = ""
			p.S3Prefix = ""
			p.Level = levelRegions
			p.Cursor = 0
			p.Marked = map[string]bool{}
			return true, true
		}
	case levelRegions:
		if it.Kind == "region" {
			p.Region = it.Value
			p.Service = ""
			p.S3Bucket = ""
			p.S3Prefix = ""
			p.Level = levelServices
			p.Cursor = 0
			p.Marked = map[string]bool{}
			return true, true
		}
	case levelServices:
		if it.Kind == "service" {
			p.Service = it.Value
			p.Level = levelResources
			p.S3Bucket = ""
			p.S3Prefix = ""
			p.Cursor = 0
			p.Marked = map[string]bool{}
			return true, true
		}
	case levelResources:
		if p.Service != "s3" {
			return false, false
		}
		switch it.Kind {
		case "bucket":
			p.S3Bucket = it.Value
			p.S3Prefix = ""
			p.Cursor = 0
			p.Marked = map[string]bool{}
			return true, true
		case "prefix":
			p.S3Prefix = it.Value
			p.Cursor = 0
			return true, true
		case "back":
			if p.S3Prefix != "" {
				p.S3Prefix = parentPrefix(p.S3Prefix)
			} else if p.S3Bucket != "" {
				p.S3Bucket = ""
			}
			p.Cursor = 0
			p.Marked = map[string]bool{}
			return true, true
		}
	}
	return false, false
}

func stepBack(p *panelState) {
	switch p.Level {
	case levelResources:
		if p.Service == "s3" {
			if p.S3Prefix != "" {
				p.S3Prefix = parentPrefix(p.S3Prefix)
				p.Cursor = 0
				return
			}
			if p.S3Bucket != "" {
				p.S3Bucket = ""
				p.Cursor = 0
				return
			}
		}
		p.Level = levelServices
		p.Service = ""
		p.Cursor = 0
	case levelServices:
		p.Level = levelRegions
		p.Region = ""
		p.Cursor = 0
	case levelRegions:
		p.Level = levelProfiles
		p.Profile = ""
		p.Cursor = 0
	}
	p.Marked = map[string]bool{}
}

func resetToStart(p *panelState) {
	p.Level = levelProfiles
	p.Profile = ""
	p.Region = ""
	p.Service = ""
	p.S3Bucket = ""
	p.S3Prefix = ""
	p.Resources = nil
	p.Marked = map[string]bool{}
	p.Cursor = 0
	p.Loading = false
	p.Err = ""
}

func (m model) updateInput(msg tea.KeyMsg) (model, tea.Cmd) {
	var cmd tea.Cmd
	m.input.Input, cmd = m.input.Input.Update(msg)
	switch msg.String() {
	case "esc":
		m.mode = modeNormal
		m.status = "Canceled"
		return m, nil
	case "enter":
		value := strings.TrimSpace(m.input.Input.Value())
		if value == "" {
			m.status = "Input required"
			return m, nil
		}
		m.active = m.input.PanelID
		m.persistContext()
		target := m.input.Target
		extra := m.input.Extra
		m.mode = modeNormal
		return m, m.runOp(target, value, extra)
	}
	return m, cmd
}

func (m model) View() string {
	if m.width == 0 {
		return "Loading..."
	}
	if m.mode == modeView {
		return renderViewMode(m)
	}
	if m.mode == modeInput {
		return renderInput(m)
	}
	if m.mode == modeConfirm {
		return renderConfirm(m)
	}
	if m.mode == modeConflict {
		return renderConflict(m)
	}
	if m.mode == modeLambdaConflict {
		return renderLambdaConflict(m)
	}
	if m.mode == modeGrantRegistry {
		return renderGrantRegistry(m)
	}
	return renderMain(m)
}

func renderMain(m model) string {
	header := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("39")).Render("AWS Commander")
	logHeight := 8
	panelWidth := max(40, m.width/2-2)
	panelHeight := max(8, m.height-logHeight-6)
	left := renderPanel("LEFT", m.left, m.active == leftPanel, panelWidth, panelHeight)
	right := renderPanel("RIGHT", m.right, m.active == rightPanel, panelWidth, panelHeight)
	body := lipgloss.JoinHorizontal(lipgloss.Top, left, right)
	shortcuts := lipgloss.NewStyle().Foreground(lipgloss.Color("246")).Render("tab switch | i home | j/k move | enter drill | h/backspace up | space mark item | c copy | m move | a grants registry | u revoke (registry) | v view | e last error | l full logs | d delete | n create bucket | g refresh | q quit")
	status := lipgloss.NewStyle().Foreground(lipgloss.Color("86")).Render("Status: " + m.status)
	logPane := renderLogPane(m, logHeight)
	return strings.Join([]string{header, body, shortcuts, status, logPane}, "\n")
}

func renderPanel(title string, p panelState, active bool, width, height int) string {
	borderColor := "240"
	if active {
		borderColor = "39"
	}
	box := lipgloss.NewStyle().Border(lipgloss.NormalBorder()).BorderForeground(lipgloss.Color(borderColor)).Padding(0, 1).Width(width).Height(height)

	head := breadcrumb(p)
	if p.Loading {
		head += " [loading]"
	}

	rows := max(1, height-5)
	lines := make([]string, 0, rows)
	start := 0
	if len(p.Resources) > rows {
		start = p.Cursor - rows/2
		if start < 0 {
			start = 0
		}
		maxStart := len(p.Resources) - rows
		if start > maxStart {
			start = maxStart
		}
	}
	end := start + rows
	if end > len(p.Resources) {
		end = len(p.Resources)
	}
	for i := start; i < end; i++ {
		it := p.Resources[i]
		cursor := "  "
		if i == p.Cursor {
			cursor = "▸ "
		}
		mark := " "
		if p.Marked[it.Value] {
			mark = "*"
		}
		lines = append(lines, fmt.Sprintf("%s%s %s", cursor, mark, it.Label))
	}
	for len(lines) < rows {
		lines = append(lines, "~")
	}
	if p.Err != "" {
		lines[len(lines)-1] = "ERR: " + firstLine(p.Err)
	}

	content := lipgloss.NewStyle().Bold(true).Render(title) + "\n" + head + "\n\n" + strings.Join(lines, "\n")
	return box.Render(content)
}

func renderInput(m model) string {
	box := lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).BorderForeground(lipgloss.Color("39")).Padding(1, 2)
	return box.Render(m.input.Title + "\n\n" + m.input.Prompt + ":\n" + m.input.Input.View() + "\n\nenter=apply esc=cancel")
}

func renderViewMode(m model) string {
	box := lipgloss.NewStyle().Border(lipgloss.NormalBorder()).BorderForeground(lipgloss.Color("39")).Padding(1, 2).Width(max(80, m.width-4)).Height(max(20, m.height-4))
	return box.Render("Resource Detail\n\n" + m.viewText + "\n\nesc/q/enter to close")
}

func renderConfirm(m model) string {
	box := lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).BorderForeground(lipgloss.Color("214")).Padding(1, 2)
	return box.Render(m.confirm.Title + "\n")
}

func renderConflict(m model) string {
	box := lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).BorderForeground(lipgloss.Color("214")).Padding(1, 2).Width(max(70, m.width-4))
	return box.Render("Copy Conflict\n\n" + m.conflict.Title + "\n\n[o] overwrite all\n[s] skip existing\n[esc] cancel")
}

func renderLambdaConflict(m model) string {
	box := lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).BorderForeground(lipgloss.Color("214")).Padding(1, 2).Width(max(70, m.width-4))
	return box.Render("Lambda Copy Conflict\n\n" + m.lconf.Title + "\n\n[o] overwrite all\n[s] skip existing\n[esc] cancel")
}

func renderGrantRegistry(m model) string {
	box := lipgloss.NewStyle().Border(lipgloss.NormalBorder()).BorderForeground(lipgloss.Color("39")).Padding(0, 1).Width(max(80, m.width-2)).Height(max(14, m.height-2))
	lines := make([]string, 0, len(m.grants.Items)+2)
	lines = append(lines, "Created Grants Registry")
	lines = append(lines, "j/k move | u revoke selected | v/enter detail | esc/q/a close")
	if len(m.grants.Items) == 0 {
		lines = append(lines, "")
		lines = append(lines, "(no active grants)")
		return box.Render(strings.Join(lines, "\n"))
	}
	for i, r := range m.grants.Items {
		cursor := "  "
		if i == m.grants.Cursor {
			cursor = "▸ "
		}
		lines = append(lines, cursor+grantRecordLabel(r))
	}
	return box.Render(strings.Join(lines, "\n"))
}

func renderLogPane(m model, height int) string {
	width := max(40, m.width-2)
	box := lipgloss.NewStyle().
		Border(lipgloss.NormalBorder()).
		BorderForeground(lipgloss.Color("244")).
		Padding(0, 1).
		Width(width).
		Height(height)

	rows := max(1, height-2)
	lines := make([]string, 0, rows)
	start := max(0, len(m.logLines)-rows)
	for i := start; i < len(m.logLines); i++ {
		lines = append(lines, m.logLines[i])
	}
	for len(lines) < rows {
		lines = append(lines, "")
	}

	return box.Render("LOG\n" + strings.Join(lines, "\n"))
}

func breadcrumb(p panelState) string {
	switch p.Level {
	case levelProfiles:
		return "Profiles"
	case levelRegions:
		return fmt.Sprintf("%s > Regions", p.Profile)
	case levelServices:
		return fmt.Sprintf("%s > %s > Services", p.Profile, p.Region)
	case levelResources:
		if p.Service == "s3" {
			if p.S3Bucket == "" {
				return fmt.Sprintf("%s > %s > s3 > Buckets", p.Profile, p.Region)
			}
			return fmt.Sprintf("%s > %s > s3 > %s:%s", p.Profile, p.Region, p.S3Bucket, p.S3Prefix)
		}
		return fmt.Sprintf("%s > %s > %s", p.Profile, p.Region, p.Service)
	default:
		return ""
	}
}

func loadPanelCmd(id panelID, p panelState) tea.Cmd {
	return func() tea.Msg {
		items, err := listPanelItems(context.Background(), p)
		return loadedPanelMsg{PanelID: id, Items: items, Err: err, Action: describePanelLoad(p)}
	}
}

func listPanelItems(ctx context.Context, p panelState) ([]resourceItem, error) {
	switch p.Level {
	case levelProfiles:
		profiles := listProfiles()
		if len(profiles) == 0 {
			profiles = []string{"default"}
		}
		items := make([]resourceItem, 0, len(profiles))
		for _, prof := range profiles {
			items = append(items, resourceItem{Label: profileLabelWithAccount(ctx, prof), Value: prof, Kind: "profile"})
		}
		return items, nil
	case levelRegions:
		regions := regionsForProfile(p.Profile)
		items := make([]resourceItem, 0, len(regions))
		for _, r := range regions {
			items = append(items, resourceItem{Label: r, Value: r, Kind: "region"})
		}
		return items, nil
	case levelServices:
		items := make([]resourceItem, 0, len(services))
		for _, s := range services {
			items = append(items, resourceItem{Label: s, Value: s, Kind: "service"})
		}
		return items, nil
	case levelResources:
		return listServiceResources(ctx, p)
	default:
		return nil, fmt.Errorf("unknown level")
	}
}

func listServiceResources(ctx context.Context, p panelState) ([]resourceItem, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(resolveProfileCase(p.Profile)),
		config.WithRegion(p.Region),
	)
	if err != nil {
		return nil, err
	}

	switch p.Service {
	case "s3":
		cli := s3.NewFromConfig(cfg)
		if p.S3Bucket == "" {
			out, err := cli.ListBuckets(ctx, &s3.ListBucketsInput{})
			if err != nil {
				return nil, err
			}
			items := make([]resourceItem, 0, len(out.Buckets))
			for _, b := range out.Buckets {
				if b.Name != nil {
					items = append(items, resourceItem{Label: *b.Name, Value: *b.Name, Kind: "bucket"})
				}
			}
			sort.Slice(items, func(i, j int) bool { return items[i].Label < items[j].Label })
			return items, nil
		}

		out, err := cli.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:    aws.String(p.S3Bucket),
			Prefix:    aws.String(p.S3Prefix),
			Delimiter: aws.String("/"),
		})
		if err != nil {
			return nil, err
		}
		items := []resourceItem{{Label: "../", Value: "..", Kind: "back"}}
		for _, cp := range out.CommonPrefixes {
			if cp.Prefix == nil {
				continue
			}
			full := *cp.Prefix
			rel := strings.TrimPrefix(full, p.S3Prefix)
			if rel != "" {
				items = append(items, resourceItem{Label: rel, Value: full, Kind: "prefix"})
			}
		}
		for _, obj := range out.Contents {
			if obj.Key == nil {
				continue
			}
			full := *obj.Key
			if full == p.S3Prefix {
				continue
			}
			rel := strings.TrimPrefix(full, p.S3Prefix)
			if rel == "" || strings.Contains(rel, "/") {
				continue
			}
			items = append(items, resourceItem{Label: rel, Value: full, Kind: "object"})
		}
		sort.SliceStable(items[1:], func(i, j int) bool {
			a := items[i+1]
			b := items[j+1]
			if a.Kind != b.Kind {
				if a.Kind == "prefix" {
					return true
				}
				if b.Kind == "prefix" {
					return false
				}
			}
			return a.Label < b.Label
		})
		return items, nil
	case "lambda":
		cli := lambda.NewFromConfig(cfg)
		out, err := cli.ListFunctions(ctx, &lambda.ListFunctionsInput{})
		if err != nil {
			return nil, err
		}
		items := make([]resourceItem, 0, len(out.Functions))
		for _, fn := range out.Functions {
			if fn.FunctionName != nil {
				items = append(items, resourceItem{Label: *fn.FunctionName, Value: *fn.FunctionName, Kind: "generic"})
			}
		}
		sort.Slice(items, func(i, j int) bool { return items[i].Label < items[j].Label })
		return items, nil
	case "vpc":
		cli := ec2.NewFromConfig(cfg)
		out, err := cli.DescribeVpcs(ctx, &ec2.DescribeVpcsInput{})
		if err != nil {
			return nil, err
		}
		items := make([]resourceItem, 0, len(out.Vpcs))
		for _, v := range out.Vpcs {
			if v.VpcId != nil {
				items = append(items, resourceItem{Label: *v.VpcId, Value: *v.VpcId, Kind: "generic"})
			}
		}
		sort.Slice(items, func(i, j int) bool { return items[i].Label < items[j].Label })
		return items, nil
	case "apigateway":
		cli := apigatewayv2.NewFromConfig(cfg)
		out, err := cli.GetApis(ctx, &apigatewayv2.GetApisInput{})
		if err != nil {
			return nil, err
		}
		items := make([]resourceItem, 0, len(out.Items))
		for _, api := range out.Items {
			if api.ApiId != nil {
				items = append(items, resourceItem{Label: *api.ApiId, Value: *api.ApiId, Kind: "generic"})
			}
		}
		sort.Slice(items, func(i, j int) bool { return items[i].Label < items[j].Label })
		return items, nil
	default:
		return nil, fmt.Errorf("unsupported service: %s", p.Service)
	}
}

func detailCmd(p panelState, it resourceItem) tea.Cmd {
	return func() tea.Msg {
		txt, err := resourceDetail(context.Background(), p, it)
		return detailMsg{Text: txt, Err: err, Action: describeDetail(p, it)}
	}
}

func resourceDetail(ctx context.Context, p panelState, it resourceItem) (string, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(resolveProfileCase(p.Profile)),
		config.WithRegion(p.Region),
	)
	if err != nil {
		return "", err
	}
	var payload any
	switch p.Service {
	case "s3":
		cli := s3.NewFromConfig(cfg)
		switch it.Kind {
		case "bucket":
			out, err := cli.GetBucketLocation(ctx, &s3.GetBucketLocationInput{Bucket: aws.String(it.Value)})
			if err != nil {
				return "", err
			}
			payload = out
		case "object":
			out, err := cli.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(p.S3Bucket), Key: aws.String(it.Value)})
			if err != nil {
				return "", err
			}
			payload = map[string]any{"bucket": p.S3Bucket, "key": it.Value, "head": out}
		default:
			payload = map[string]any{"item": it}
		}
	case "lambda":
		cli := lambda.NewFromConfig(cfg)
		out, err := cli.GetFunction(ctx, &lambda.GetFunctionInput{FunctionName: aws.String(it.Value)})
		if err != nil {
			return "", err
		}
		payload = out
	case "vpc":
		cli := ec2.NewFromConfig(cfg)
		out, err := cli.DescribeVpcs(ctx, &ec2.DescribeVpcsInput{VpcIds: []string{it.Value}})
		if err != nil {
			return "", err
		}
		payload = out
	case "apigateway":
		cli := apigatewayv2.NewFromConfig(cfg)
		out, err := cli.GetApi(ctx, &apigatewayv2.GetApiInput{ApiId: aws.String(it.Value)})
		if err != nil {
			return "", err
		}
		payload = out
	default:
		payload = map[string]any{"item": it}
	}
	b, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func copyObjectsCmd(src panelState, dst panelState, keys []string) tea.Cmd {
	return func() tea.Msg {
		action := fmt.Sprintf("copy s3 objects %s/%s -> %s/%s (%d)", src.Profile, src.S3Bucket, dst.Profile, dst.S3Bucket, len(keys))
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		cfg, err := config.LoadDefaultConfig(ctx,
			config.WithSharedConfigProfile(resolveProfileCase(dst.Profile)),
			config.WithRegion(dst.Region),
		)
		if err != nil {
			return opMsg{Err: err, Action: action}
		}
		cli := s3.NewFromConfig(cfg)
		copied := 0
		failed := make([]string, 0)
		for _, key := range keys {
			copySource := encodeCopySource(src.S3Bucket, key)
			_, err := cli.CopyObject(ctx, &s3.CopyObjectInput{
				Bucket:     aws.String(dst.S3Bucket),
				Key:        aws.String(key),
				CopySource: aws.String(copySource),
			})
			if err != nil {
				failed = append(failed, fmt.Sprintf("%s (%v)", key, err))
				continue
			}
			copied++
		}
		if len(failed) > 0 {
			first := failed[0]
			return opMsg{
				Err:    fmt.Errorf("partial copy: copied %d/%d objects, failed %d. first failure: %s", copied, len(keys), len(failed), first),
				Action: action,
			}
		}
		return opMsg{Status: fmt.Sprintf("Copied %d object(s) to %s/%s", copied, dst.Profile, dst.S3Bucket), Action: action}
	}
}

func moveObjectsCmd(src panelState, dst panelState, keys []string) tea.Cmd {
	return func() tea.Msg {
		action := fmt.Sprintf("move s3 objects %s/%s -> %s/%s (%d)", src.Profile, src.S3Bucket, dst.Profile, dst.S3Bucket, len(keys))
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()

		dstCfg, err := config.LoadDefaultConfig(ctx,
			config.WithSharedConfigProfile(resolveProfileCase(dst.Profile)),
			config.WithRegion(dst.Region),
		)
		if err != nil {
			return opMsg{Err: err, Action: action}
		}
		srcCfg, err := config.LoadDefaultConfig(ctx,
			config.WithSharedConfigProfile(resolveProfileCase(src.Profile)),
			config.WithRegion(src.Region),
		)
		if err != nil {
			return opMsg{Err: err, Action: action}
		}
		dstCli := s3.NewFromConfig(dstCfg)
		srcCli := s3.NewFromConfig(srcCfg)

		moved := 0
		failed := make([]string, 0)
		for _, key := range keys {
			copySource := encodeCopySource(src.S3Bucket, key)
			_, err := dstCli.CopyObject(ctx, &s3.CopyObjectInput{
				Bucket:     aws.String(dst.S3Bucket),
				Key:        aws.String(key),
				CopySource: aws.String(copySource),
			})
			if err != nil {
				failed = append(failed, fmt.Sprintf("copy %s (%v)", key, err))
				continue
			}
			_, err = srcCli.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket: aws.String(src.S3Bucket),
				Key:    aws.String(key),
			})
			if err != nil {
				failed = append(failed, fmt.Sprintf("delete source %s (%v)", key, err))
				continue
			}
			moved++
		}
		if len(failed) > 0 {
			first := failed[0]
			return opMsg{
				Err:    fmt.Errorf("partial move: moved %d/%d objects, failed %d. first failure: %s", moved, len(keys), len(failed), first),
				Action: action,
			}
		}
		return opMsg{Status: fmt.Sprintf("Moved %d object(s)", moved), Action: action}
	}
}

func copyBucketObjectsCmd(src panelState, dst panelState, buckets []string) tea.Cmd {
	return func() tea.Msg {
		action := fmt.Sprintf("copy bucket content(s) -> %s/%s (%d bucket(s))", dst.Profile, dst.S3Bucket, len(buckets))
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		total := 0
		for _, b := range buckets {
			keys, err := listAllObjectKeys(ctx, src.Profile, src.Region, b)
			if err != nil {
				return opMsg{Err: fmt.Errorf("list objects %s: %w", b, err), Action: action}
			}
			srcTmp := src
			srcTmp.S3Bucket = b
			dstTmp := dst
			msg := copyObjectsCmd(srcTmp, dstTmp, keys)().(opMsg)
			if msg.Err != nil {
				return msg
			}
			total += len(keys)
		}
		return opMsg{Status: fmt.Sprintf("Copied %d object(s) from %d bucket(s)", total, len(buckets)), Action: action}
	}
}

func moveBucketObjectsCmd(src panelState, dst panelState, buckets []string) tea.Cmd {
	return func() tea.Msg {
		action := fmt.Sprintf("move bucket content(s) -> %s/%s (%d bucket(s))", dst.Profile, dst.S3Bucket, len(buckets))
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()
		total := 0
		for _, b := range buckets {
			keys, err := listAllObjectKeys(ctx, src.Profile, src.Region, b)
			if err != nil {
				return opMsg{Err: fmt.Errorf("list objects %s: %w", b, err), Action: action}
			}
			srcTmp := src
			srcTmp.S3Bucket = b
			dstTmp := dst
			msg := moveObjectsCmd(srcTmp, dstTmp, keys)().(opMsg)
			if msg.Err != nil {
				return msg
			}
			if err := deleteS3Bucket(ctx, src.Profile, src.Region, b); err != nil {
				return opMsg{Err: fmt.Errorf("delete source bucket %s: %w", b, err), Action: action}
			}
			total += len(keys)
		}
		return opMsg{Status: fmt.Sprintf("Moved %d object(s) from %d bucket(s)", total, len(buckets)), Action: action}
	}
}

func copyWholeBucketToNewCmd(srcProfile, srcRegion, srcBucket, dstProfile, dstRegion, dstBucket string, move bool) tea.Cmd {
	return func() tea.Msg {
		modeLabel := "copy"
		if move {
			modeLabel = "move"
		}
		action := fmt.Sprintf("%s full bucket %s/%s -> %s/%s", modeLabel, srcProfile, srcBucket, dstProfile, dstBucket)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
		defer cancel()

		if err := createS3Bucket(ctx, dstProfile, dstRegion, dstBucket); err != nil {
			return opMsg{Err: err, Action: action}
		}
		keys, err := listAllObjectKeys(ctx, srcProfile, srcRegion, srcBucket)
		if err != nil {
			return opMsg{Err: err, Action: action}
		}
		src := panelState{Profile: srcProfile, Region: srcRegion, Service: "s3", S3Bucket: srcBucket}
		dst := panelState{Profile: dstProfile, Region: dstRegion, Service: "s3", S3Bucket: dstBucket}
		msg := copyObjectsCmd(src, dst, keys)().(opMsg)
		if msg.Err != nil {
			return msg
		}
		if move {
			if err := deleteAllObjectsInBucket(ctx, srcProfile, srcRegion, srcBucket); err != nil {
				return opMsg{Err: err, Action: action}
			}
			if err := deleteS3Bucket(ctx, srcProfile, srcRegion, srcBucket); err != nil {
				return opMsg{Err: err, Action: action}
			}
		}
		if move {
			return opMsg{Status: fmt.Sprintf("Moved bucket %s to %s", srcBucket, dstBucket), Action: action}
		}
		return opMsg{Status: fmt.Sprintf("Copied bucket %s to %s", srcBucket, dstBucket), Action: action}
	}
}

func deleteBucketFullCmd(panel panelState, bucket string) tea.Cmd {
	return func() tea.Msg {
		action := fmt.Sprintf("delete full bucket %s", bucket)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()
		if err := deleteAllObjectsInBucket(ctx, panel.Profile, panel.Region, bucket); err != nil {
			return opMsg{Err: err, Action: action}
		}
		if err := deleteS3Bucket(ctx, panel.Profile, panel.Region, bucket); err != nil {
			return opMsg{Err: err, Action: action}
		}
		return opMsg{Status: "Bucket deleted with all objects: " + bucket, Action: action}
	}
}

func listAllObjectKeys(ctx context.Context, profile, region, bucket string) ([]string, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(resolveProfileCase(profile)),
		config.WithRegion(region),
	)
	if err != nil {
		return nil, err
	}
	cli := s3.NewFromConfig(cfg)
	keys := make([]string, 0, 1024)
	var token *string
	for {
		out, err := cli.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			ContinuationToken: token,
		})
		if err != nil {
			return nil, err
		}
		for _, o := range out.Contents {
			if o.Key != nil {
				keys = append(keys, *o.Key)
			}
		}
		if out.IsTruncated == nil || !*out.IsTruncated || out.NextContinuationToken == nil {
			break
		}
		token = out.NextContinuationToken
	}
	return keys, nil
}

func listAllObjectKeysUnderPrefix(ctx context.Context, profile, region, bucket, prefix string) ([]string, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(resolveProfileCase(profile)),
		config.WithRegion(region),
	)
	if err != nil {
		return nil, err
	}
	cli := s3.NewFromConfig(cfg)
	keys := make([]string, 0, 1024)
	var token *string
	for {
		out, err := cli.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: token,
		})
		if err != nil {
			return nil, err
		}
		for _, o := range out.Contents {
			if o.Key != nil {
				keys = append(keys, *o.Key)
			}
		}
		if out.IsTruncated == nil || !*out.IsTruncated || out.NextContinuationToken == nil {
			break
		}
		token = out.NextContinuationToken
	}
	return keys, nil
}

func deleteAllObjectsInBucket(ctx context.Context, profile, region, bucket string) error {
	keys, err := listAllObjectKeys(ctx, profile, region, bucket)
	if err != nil {
		return err
	}
	if len(keys) == 0 {
		return nil
	}
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(resolveProfileCase(profile)),
		config.WithRegion(region),
	)
	if err != nil {
		return err
	}
	cli := s3.NewFromConfig(cfg)
	for i := 0; i < len(keys); i += 1000 {
		end := i + 1000
		if end > len(keys) {
			end = len(keys)
		}
		objects := make([]s3types.ObjectIdentifier, 0, end-i)
		for _, k := range keys[i:end] {
			key := k
			objects = append(objects, s3types.ObjectIdentifier{Key: aws.String(key)})
		}
		_, err := cli.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(bucket),
			Delete: &s3types.Delete{Objects: objects, Quiet: aws.Bool(true)},
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func grantS3CopyAccessCmd(src panelState, dst panelState) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		principalARN, err := callerIdentityARN(ctx, dst.Profile, dst.Region)
		action := fmt.Sprintf("grant s3 copy access source=%s/%s principal=%s", src.Profile, src.S3Bucket, principalARN)
		if err != nil {
			return opMsg{Err: err, Action: "resolve destination principal"}
		}

		srcCfg, err := config.LoadDefaultConfig(ctx,
			config.WithSharedConfigProfile(resolveProfileCase(src.Profile)),
			config.WithRegion(src.Region),
		)
		if err != nil {
			return opMsg{Err: err, Action: action}
		}
		s3cli := s3.NewFromConfig(srcCfg)

		doc, err := readBucketPolicyDocument(ctx, s3cli, src.S3Bucket)
		if err != nil {
			return opMsg{Err: err, Action: action}
		}

		now := time.Now().UTC()
		sidBase := formatPolicySid("NCAWSCopyAccess" + now.Format("20060102T150405"))
		sidGet := sidBase + "GetObject"
		sidList := sidBase + "ListBucket"

		addPolicyStatement(doc, map[string]any{
			"Sid":    sidGet,
			"Effect": "Allow",
			"Principal": map[string]any{
				"AWS": principalARN,
			},
			"Action":   []string{"s3:GetObject", "s3:GetObjectVersion"},
			"Resource": fmt.Sprintf("arn:aws:s3:::%s/*", src.S3Bucket),
		})

		addPolicyStatement(doc, map[string]any{
			"Sid":    sidList,
			"Effect": "Allow",
			"Principal": map[string]any{
				"AWS": principalARN,
			},
			"Action":   "s3:ListBucket",
			"Resource": fmt.Sprintf("arn:aws:s3:::%s", src.S3Bucket),
		})

		if err := putBucketPolicyDocument(ctx, s3cli, src.S3Bucket, doc); err != nil {
			return opMsg{Err: err, Action: action}
		}

		rec := policyGrantRecord{
			GrantType:               "s3",
			ID:                      sidBase,
			CreatedAt:               now.Format(time.RFC3339),
			SourceProfile:           src.Profile,
			SourceRegion:            src.Region,
			SourceBucket:            src.S3Bucket,
			DestinationProfile:      dst.Profile,
			DestinationRegion:       dst.Region,
			DestinationPrincipalARN: principalARN,
			SidGetObject:            sidGet,
			SidListBucket:           sidList,
		}
		if err := appendPolicyGrantRecord(rec); err != nil {
			return opMsg{Err: err, Action: action}
		}

		return opMsg{
			Status: fmt.Sprintf("Grant created (%s) for principal %s on bucket %s", sidBase, principalARN, src.S3Bucket),
			Action: action,
		}
	}
}

func revokeS3CopyAccessCmd(src panelState, dst panelState) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		principalARN, err := callerIdentityARN(ctx, dst.Profile, dst.Region)
		action := fmt.Sprintf("revoke s3 copy access source=%s/%s principal=%s", src.Profile, src.S3Bucket, principalARN)
		if err != nil {
			return opMsg{Err: err, Action: "resolve destination principal"}
		}

		reg, err := loadPolicyRegistry()
		if err != nil {
			return opMsg{Err: err, Action: action}
		}
		idx := findLatestActiveGrantIndex(reg, src.S3Bucket, principalARN)
		if idx < 0 {
			return opMsg{Err: fmt.Errorf("no active grant found for bucket=%s principal=%s", src.S3Bucket, principalARN), Action: action}
		}
		rec := reg.Records[idx]

		srcCfg, err := config.LoadDefaultConfig(ctx,
			config.WithSharedConfigProfile(resolveProfileCase(src.Profile)),
			config.WithRegion(src.Region),
		)
		if err != nil {
			return opMsg{Err: err, Action: action}
		}
		s3cli := s3.NewFromConfig(srcCfg)

		doc, err := readBucketPolicyDocument(ctx, s3cli, src.S3Bucket)
		if err != nil {
			return opMsg{Err: err, Action: action}
		}
		removePolicyStatementBySid(doc, rec.SidGetObject)
		removePolicyStatementBySid(doc, rec.SidListBucket)
		if err := putBucketPolicyDocument(ctx, s3cli, src.S3Bucket, doc); err != nil {
			return opMsg{Err: err, Action: action}
		}

		reg.Records[idx].Revoked = true
		reg.Records[idx].RevokedAt = time.Now().UTC().Format(time.RFC3339)
		if err := savePolicyRegistry(reg); err != nil {
			return opMsg{Err: err, Action: action}
		}

		return opMsg{
			Status: fmt.Sprintf("Grant revoked (%s)", rec.ID),
			Action: action,
		}
	}
}

func grantLambdaCopyAccessCmd(src panelState, dst panelState) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		srcFn, err := selectedOrFocusedLambdaName(src)
		if err != nil {
			return opMsg{Err: err, Action: "resolve source lambda"}
		}
		srcCfg, err := config.LoadDefaultConfig(ctx,
			config.WithSharedConfigProfile(resolveProfileCase(src.Profile)),
			config.WithRegion(src.Region),
		)
		if err != nil {
			return opMsg{Err: err, Action: "load source config"}
		}
		srcCli := lambda.NewFromConfig(srcCfg)
		out, err := srcCli.GetFunction(ctx, &lambda.GetFunctionInput{FunctionName: aws.String(srcFn)})
		if err != nil {
			return opMsg{Err: err, Action: "read source lambda role"}
		}
		if out.Configuration == nil || out.Configuration.Role == nil || strings.TrimSpace(*out.Configuration.Role) == "" {
			return opMsg{Err: fmt.Errorf("source lambda has no role"), Action: "read source lambda role"}
		}
		roleArn := *out.Configuration.Role
		targetRoleArn, roleCreated, roleManagedPolicyArns, err := resolveOrCreateDestinationLambdaRoleARN(ctx, src, dst, roleArn)
		if err != nil {
			return opMsg{Err: err, Action: "resolve destination role"}
		}
		if err := ensureLambdaPassRoleGrant(ctx, dst, targetRoleArn, roleCreated, roleManagedPolicyArns); err != nil {
			return opMsg{Err: err, Action: "grant lambda copy access"}
		}
		return opMsg{
			Status: fmt.Sprintf("Lambda copy grant ready for role %s", targetRoleArn),
			Action: "grant lambda copy access",
		}
	}
}

func revokeLambdaCopyAccessCmd(src panelState, dst panelState) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		srcFn, err := selectedOrFocusedLambdaName(src)
		if err != nil {
			return opMsg{Err: err, Action: "resolve source lambda"}
		}
		srcCfg, err := config.LoadDefaultConfig(ctx,
			config.WithSharedConfigProfile(resolveProfileCase(src.Profile)),
			config.WithRegion(src.Region),
		)
		if err != nil {
			return opMsg{Err: err, Action: "load source config"}
		}
		srcCli := lambda.NewFromConfig(srcCfg)
		out, err := srcCli.GetFunction(ctx, &lambda.GetFunctionInput{FunctionName: aws.String(srcFn)})
		if err != nil {
			return opMsg{Err: err, Action: "read source lambda role"}
		}
		if out.Configuration == nil || out.Configuration.Role == nil || strings.TrimSpace(*out.Configuration.Role) == "" {
			return opMsg{Err: fmt.Errorf("source lambda has no role"), Action: "read source lambda role"}
		}
		roleArn := *out.Configuration.Role
		targetRoleArn, _, _, err := resolveOrCreateDestinationLambdaRoleARN(ctx, src, dst, roleArn)
		if err != nil {
			return opMsg{Err: err, Action: "resolve destination role"}
		}

		principalArn, err := callerIdentityARN(ctx, dst.Profile, dst.Region)
		if err != nil {
			return opMsg{Err: err, Action: "resolve destination principal"}
		}
		reg, err := loadPolicyRegistry()
		if err != nil {
			return opMsg{Err: err, Action: "load policy registry"}
		}
		idx := findLatestActiveLambdaGrantIndex(reg, dst.Profile, dst.Region, principalArn, targetRoleArn)
		if idx < 0 {
			return opMsg{Err: fmt.Errorf("no active lambda grant found for principal=%s role=%s", principalArn, targetRoleArn), Action: "revoke lambda copy access"}
		}
		rec := reg.Records[idx]

		dstCfg, err := config.LoadDefaultConfig(ctx,
			config.WithSharedConfigProfile(resolveProfileCase(dst.Profile)),
			config.WithRegion(dst.Region),
		)
		if err != nil {
			return opMsg{Err: err, Action: "load destination config"}
		}
		iamCli := iam.NewFromConfig(dstCfg)
		pt, pn, err := principalAttachmentTarget(principalArn)
		if err != nil {
			return opMsg{Err: err, Action: "resolve principal target"}
		}
		if err := detachPolicyFromPrincipal(ctx, iamCli, pt, pn, rec.IamPolicyArn); err != nil {
			return opMsg{Err: err, Action: "detach lambda grant policy"}
		}
		if _, err := iamCli.DeletePolicy(ctx, &iam.DeletePolicyInput{PolicyArn: aws.String(rec.IamPolicyArn)}); err != nil {
			return opMsg{Err: err, Action: "delete lambda grant policy"}
		}
		if rec.LambdaRoleCreated && rec.LambdaRoleArn != "" {
			if err := deleteRoleCompletely(ctx, iamCli, rec.LambdaRoleArn, rec.LambdaRoleManagedPolicyArns); err != nil {
				return opMsg{Err: err, Action: "delete lambda destination role"}
			}
		}

		reg.Records[idx].Revoked = true
		reg.Records[idx].RevokedAt = time.Now().UTC().Format(time.RFC3339)
		if err := savePolicyRegistry(reg); err != nil {
			return opMsg{Err: err, Action: "persist policy registry"}
		}
		return opMsg{Status: fmt.Sprintf("Lambda grant revoked (%s)", rec.ID), Action: "revoke lambda copy access"}
	}
}

func createS3Bucket(ctx context.Context, profile, region, name string) error {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(resolveProfileCase(profile)),
		config.WithRegion(region),
	)
	if err != nil {
		return err
	}
	cli := s3.NewFromConfig(cfg)
	_, err = cli.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(name)})
	return err
}

func deleteS3Bucket(ctx context.Context, profile, region, bucket string) error {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(resolveProfileCase(profile)),
		config.WithRegion(region),
	)
	if err != nil {
		return err
	}
	cli := s3.NewFromConfig(cfg)
	_, err = cli.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
	return err
}

func deleteS3Object(ctx context.Context, profile, region, bucket, key string) error {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(resolveProfileCase(profile)),
		config.WithRegion(region),
	)
	if err != nil {
		return err
	}
	cli := s3.NewFromConfig(cfg)
	_, err = cli.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	return err
}

func deleteLambda(ctx context.Context, profile, region, fn string) error {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(resolveProfileCase(profile)),
		config.WithRegion(region),
	)
	if err != nil {
		return err
	}
	cli := lambda.NewFromConfig(cfg)
	_, err = cli.DeleteFunction(ctx, &lambda.DeleteFunctionInput{FunctionName: aws.String(fn)})
	return err
}

func opCmd(panel panelState, target, payload, extra string) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		switch target {
		case "create_s3_bucket":
			action := fmt.Sprintf("create s3 bucket %s profile=%s region=%s", payload, panel.Profile, panel.Region)
			err := createS3Bucket(ctx, panel.Profile, panel.Region, payload)
			if err != nil {
				return opMsg{Err: err, Action: action}
			}
			return opMsg{Status: "Bucket created: " + payload, Action: action}
		case "delete_s3_bucket":
			action := fmt.Sprintf("delete s3 bucket %s profile=%s region=%s", payload, panel.Profile, panel.Region)
			err := deleteS3Bucket(ctx, panel.Profile, panel.Region, payload)
			if err != nil {
				return opMsg{Err: err, Action: action}
			}
			return opMsg{Status: "Bucket deleted: " + payload, Action: action}
		case "delete_s3_bucket_full":
			return deleteBucketFullCmd(panel, payload)()
		case "delete_s3_object":
			action := fmt.Sprintf("delete s3 object %s/%s profile=%s region=%s", panel.S3Bucket, payload, panel.Profile, panel.Region)
			err := deleteS3Object(ctx, panel.Profile, panel.Region, panel.S3Bucket, payload)
			if err != nil {
				return opMsg{Err: err, Action: action}
			}
			return opMsg{Status: "Object deleted: " + payload, Action: action}
		case "copy_bucket_new":
			srcBucket, dstProfile, dstRegion, err := parseBucketInputExtra(extra)
			if err != nil {
				return opMsg{Err: err, Action: "copy bucket to new"}
			}
			return copyWholeBucketToNewCmd(panel.Profile, panel.Region, srcBucket, dstProfile, dstRegion, payload, false)()
		case "move_bucket_new":
			srcBucket, dstProfile, dstRegion, err := parseBucketInputExtra(extra)
			if err != nil {
				return opMsg{Err: err, Action: "move bucket to new"}
			}
			return copyWholeBucketToNewCmd(panel.Profile, panel.Region, srcBucket, dstProfile, dstRegion, payload, true)()
		case "delete_lambda":
			action := fmt.Sprintf("delete lambda %s profile=%s region=%s", payload, panel.Profile, panel.Region)
			err := deleteLambda(ctx, panel.Profile, panel.Region, payload)
			if err != nil {
				return opMsg{Err: err, Action: action}
			}
			return opMsg{Status: "Lambda deleted: " + payload, Action: action}
		case "delete_lambda_bulk":
			names := make([]string, 0)
			for _, n := range strings.Split(payload, "\n") {
				n = strings.TrimSpace(n)
				if n != "" {
					names = append(names, n)
				}
			}
			action := fmt.Sprintf("delete lambdas (%d) profile=%s region=%s", len(names), panel.Profile, panel.Region)
			if len(names) == 0 {
				return opMsg{Status: "No lambdas selected", Action: action}
			}
			ctxBulk, cancelBulk := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancelBulk()
			ok := 0
			failed := make([]string, 0)
			for _, fn := range names {
				if err := deleteLambda(ctxBulk, panel.Profile, panel.Region, fn); err != nil {
					failed = append(failed, fmt.Sprintf("%s (%v)", fn, err))
					continue
				}
				ok++
			}
			if len(failed) > 0 {
				first := failed[0]
				return opMsg{
					Err:    fmt.Errorf("partial delete: deleted %d/%d lambdas, failed %d. first failure: %s", ok, len(names), len(failed), first),
					Action: action,
				}
			}
			return opMsg{Status: fmt.Sprintf("Deleted %d lambda(s)", ok), Action: action}
		default:
			return opMsg{Err: fmt.Errorf("unsupported operation: %s", target), Action: "unsupported operation"}
		}
	}
}

func newInputState(title, prompt, target string, pid panelID, defaultValue, extra string) inputState {
	ti := textinput.New()
	ti.SetValue(defaultValue)
	ti.Focus()
	ti.CharLimit = 256
	ti.Width = 48
	return inputState{Title: title, Prompt: prompt, Target: target, PanelID: pid, Extra: extra, Input: ti}
}

func (m model) activePanel() panelState {
	if m.active == leftPanel {
		return m.left
	}
	return m.right
}

func (m *model) setActivePanel(p panelState) {
	if m.active == leftPanel {
		m.left = p
	} else {
		m.right = p
	}
}

func (m model) getPanel(id panelID) panelState {
	if id == leftPanel {
		return m.left
	}
	return m.right
}

func (m *model) setPanel(id panelID, p panelState) {
	if p.Marked == nil {
		p.Marked = map[string]bool{}
	}
	if id == leftPanel {
		m.left = p
	} else {
		m.right = p
	}
}

func (m model) runOp(target, payload, extra string) tea.Cmd {
	panel := m.getPanel(m.active)
	return opCmd(panel, target, payload, extra)
}

func opposite(id panelID) panelID {
	if id == leftPanel {
		return rightPanel
	}
	return leftPanel
}

func keepOnlyVisibleMarks(marked map[string]bool, items []resourceItem) map[string]bool {
	out := map[string]bool{}
	if len(marked) == 0 {
		return out
	}
	visible := map[string]bool{}
	for _, it := range items {
		if it.Kind == "object" || it.Kind == "bucket" || it.Kind == "generic" {
			visible[it.Value] = true
		}
	}
	for k := range marked {
		if visible[k] {
			out[k] = true
		}
	}
	return out
}

func selectedItem(p panelState) (resourceItem, bool) {
	if p.Cursor < 0 || p.Cursor >= len(p.Resources) {
		return resourceItem{}, false
	}
	return p.Resources[p.Cursor], true
}

func selectedObjectKeys(p panelState) []string {
	out := make([]string, 0, len(p.Marked))
	for _, it := range p.Resources {
		if it.Kind == "object" && p.Marked[it.Value] {
			out = append(out, it.Value)
		}
	}
	return out
}

func selectedGenericValues(p panelState) []string {
	out := make([]string, 0, len(p.Marked))
	for _, it := range p.Resources {
		if it.Kind == "generic" && p.Marked[it.Value] {
			out = append(out, it.Value)
		}
	}
	return out
}

func markAllSelectableItems(p *panelState) int {
	if p.Marked == nil {
		p.Marked = map[string]bool{}
	}
	count := 0
	for _, it := range p.Resources {
		markable := false
		if p.Service == "s3" {
			markable = it.Kind == "object" || it.Kind == "bucket"
		} else {
			markable = it.Kind == "generic"
		}
		if !markable {
			continue
		}
		p.Marked[it.Value] = true
		count++
	}
	return count
}

func (m *model) startCopyJob(src, dst panelState, keys []string, move bool) tea.Cmd {
	if len(keys) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	dstCfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(resolveProfileCase(dst.Profile)),
		config.WithRegion(dst.Region),
	)
	if err != nil {
		m.status = "AWS config error (destination): " + firstLine(err.Error())
		m.lastInfo = err.Error()
		m.pushLog("ERROR start copy job :: " + err.Error())
		return nil
	}
	dstCli := s3.NewFromConfig(dstCfg)
	var srcCli *s3.Client
	if move {
		srcCfg, err := config.LoadDefaultConfig(ctx,
			config.WithSharedConfigProfile(resolveProfileCase(src.Profile)),
			config.WithRegion(src.Region),
		)
		if err != nil {
			m.status = "AWS config error (source): " + firstLine(err.Error())
			m.lastInfo = err.Error()
			m.pushLog("ERROR start move job :: " + err.Error())
			return nil
		}
		srcCli = s3.NewFromConfig(srcCfg)
	}
	job := copyJobState{
		Src:    src,
		Dst:    dst,
		Keys:   append([]string(nil), keys...),
		Move:   move,
		Index:  0,
		DstCli: dstCli,
		SrcCli: srcCli,
	}
	m.copyJob = &job
	label := "copy"
	if move {
		label = "move"
	}
	first := shortKey(keys[0], 72)
	if move {
		m.status = fmt.Sprintf("Moving 1/%d :: %s", len(keys), first)
	} else {
		m.status = fmt.Sprintf("Copying 1/%d :: %s", len(keys), first)
	}
	m.pushLog(fmt.Sprintf("START %s s3 objects %s/%s -> %s/%s (%d)", label, src.Profile, src.S3Bucket, dst.Profile, dst.S3Bucket, len(keys)))
	return copyStepCmd(job)
}

func (m *model) startLambdaCopyJob(src, dst panelState, names []string, skipExisting bool, existing map[string]bool) tea.Cmd {
	if len(names) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	srcCfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(resolveProfileCase(src.Profile)),
		config.WithRegion(src.Region),
	)
	if err != nil {
		m.status = "AWS config error (lambda source): " + firstLine(err.Error())
		m.lastInfo = err.Error()
		m.pushLog("ERROR start lambda copy job :: " + err.Error())
		return nil
	}
	dstCfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(resolveProfileCase(dst.Profile)),
		config.WithRegion(dst.Region),
	)
	if err != nil {
		m.status = "AWS config error (lambda destination): " + firstLine(err.Error())
		m.lastInfo = err.Error()
		m.pushLog("ERROR start lambda copy job :: " + err.Error())
		return nil
	}
	if existing == nil {
		existing = map[string]bool{}
	}
	dstPrincipalArn, err := callerIdentityARN(ctx, dst.Profile, dst.Region)
	if err != nil {
		m.status = "Resolve destination principal error: " + firstLine(err.Error())
		m.lastInfo = err.Error()
		m.pushLog("ERROR start lambda copy job :: " + err.Error())
		return nil
	}
	job := lambdaCopyJobState{
		Src:              src,
		Dst:              dst,
		Names:            append([]string(nil), names...),
		SkipExisting:     skipExisting,
		Existing:         existing,
		AutoGrantedRoles: map[string]bool{},
		DstPrincipalArn:  dstPrincipalArn,
		SrcCli:           lambda.NewFromConfig(srcCfg),
		DstCli:           lambda.NewFromConfig(dstCfg),
		DstIamCli:        iam.NewFromConfig(dstCfg),
	}
	m.lambdaJob = &job
	first := shortKey(names[0], 72)
	m.status = fmt.Sprintf("Copying lambda 1/%d :: %s", len(names), first)
	m.pushLog(fmt.Sprintf("START copy lambda(s) %s/%s -> %s/%s (%d) [skip_existing=%t]", src.Profile, src.Region, dst.Profile, dst.Region, len(names), skipExisting))
	return lambdaCopyStepCmd(job)
}

func copyStepCmd(job copyJobState) tea.Cmd {
	return func() tea.Msg {
		if job.Index < 0 || job.Index >= len(job.Keys) {
			return copyProgressMsg{Job: job, Key: "", Err: fmt.Errorf("job index out of range"), Action: "copy step"}
		}
		key := job.Keys[job.Index]
		action := "copy s3 object"
		if job.Move {
			action = "move s3 object"
		}

		ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
		defer cancel()

		copySource := encodeCopySource(job.Src.S3Bucket, key)
		var copyErr error
		for attempt := 1; attempt <= 2; attempt++ {
			_, copyErr = job.DstCli.CopyObject(ctx, &s3.CopyObjectInput{
				Bucket:     aws.String(job.Dst.S3Bucket),
				Key:        aws.String(key),
				CopySource: aws.String(copySource),
			})
			if copyErr == nil {
				break
			}
			if attempt == 1 && isRetryableCopyErr(copyErr) {
				time.Sleep(250 * time.Millisecond)
				continue
			}
			break
		}
		err := copyErr
		if err != nil {
			return copyProgressMsg{Job: job, Key: key, Err: err, Action: action}
		}

		if job.Move {
			_, err = job.SrcCli.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket: aws.String(job.Src.S3Bucket),
				Key:    aws.String(key),
			})
			if err != nil {
				return copyProgressMsg{Job: job, Key: key, Err: err, Action: action}
			}
		}

		return copyProgressMsg{Job: job, Key: key, Err: nil, Action: action}
	}
}

func lambdaCopyStepCmd(job lambdaCopyJobState) tea.Cmd {
	return func() tea.Msg {
		if job.Index < 0 || job.Index >= len(job.Names) {
			return lambdaCopyProgressMsg{Job: job, Name: "", Err: fmt.Errorf("lambda job index out of range"), Action: "copy lambda"}
		}
		name := job.Names[job.Index]
		action := "copy lambda"

		exists := job.Existing[name]
		if exists && job.SkipExisting {
			return lambdaCopyProgressMsg{Job: job, Name: name, Skipped: true, Action: action}
		}

		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
		defer cancel()

		srcFn, err := job.SrcCli.GetFunction(ctx, &lambda.GetFunctionInput{FunctionName: aws.String(name)})
		if err != nil {
			return lambdaCopyProgressMsg{Job: job, Name: name, Err: fmt.Errorf("read source: %w", err), Action: action}
		}
		if srcFn.Configuration == nil {
			return lambdaCopyProgressMsg{Job: job, Name: name, Err: fmt.Errorf("missing source configuration"), Action: action}
		}
		cfg := srcFn.Configuration
		if cfg.Role == nil || strings.TrimSpace(*cfg.Role) == "" {
			return lambdaCopyProgressMsg{Job: job, Name: name, Err: fmt.Errorf("source lambda has no role"), Action: action}
		}
		targetRoleArn, roleCreated, roleManagedPolicyArns, err := resolveOrCreateDestinationLambdaRoleARN(ctx, job.Src, job.Dst, *cfg.Role)
		if err != nil {
			return lambdaCopyProgressMsg{Job: job, Name: name, Err: err, Action: action}
		}

		if exists {
			if err := syncLambdaCode(ctx, job.DstCli, name, srcFn.Code, cfg.Architectures); err != nil {
				return lambdaCopyProgressMsg{Job: job, Name: name, Err: fmt.Errorf("update code: %w", err), Action: action}
			}
			upd := buildUpdateLambdaConfigInput(name, cfg, targetRoleArn)
			_, err = job.DstCli.UpdateFunctionConfiguration(ctx, upd)
			if err != nil && isLambdaPassRoleDeniedErr(err) && !job.AutoGrantedRoles[targetRoleArn] {
				if gerr := ensureLambdaPassRoleGrantForJob(ctx, job, targetRoleArn, roleCreated, roleManagedPolicyArns); gerr == nil {
					job.AutoGrantedRoles[targetRoleArn] = true
					_, err = job.DstCli.UpdateFunctionConfiguration(ctx, upd)
				} else {
					return lambdaCopyProgressMsg{Job: job, Name: name, Err: fmt.Errorf("auto-grant failed: %w", gerr), Action: action}
				}
			}
			if err != nil && isLambdaRoleCannotBeAssumedErr(err) {
				if roleCreated {
					time.Sleep(4 * time.Second)
					_, err = job.DstCli.UpdateFunctionConfiguration(ctx, upd)
				} else {
					if altRoleArn, altManaged, aerr := createAlternativeDestinationLambdaRole(ctx, job.Src, job.Dst, *cfg.Role, name); aerr == nil {
						targetRoleArn = altRoleArn
						roleCreated = true
						roleManagedPolicyArns = altManaged
						upd = buildUpdateLambdaConfigInput(name, cfg, targetRoleArn)
						_, err = job.DstCli.UpdateFunctionConfiguration(ctx, upd)
					}
				}
			}
			if err != nil {
				return lambdaCopyProgressMsg{Job: job, Name: name, Err: fmt.Errorf("update config: %w", err), Action: action}
			}
			return lambdaCopyProgressMsg{Job: job, Name: name, Action: action}
		}

		createInput, err := buildCreateLambdaInput(name, cfg, srcFn.Code, targetRoleArn)
		if err != nil {
			return lambdaCopyProgressMsg{Job: job, Name: name, Err: fmt.Errorf("prepare create: %w", err), Action: action}
		}
		_, err = job.DstCli.CreateFunction(ctx, createInput)
		if err != nil && isLambdaPassRoleDeniedErr(err) && !job.AutoGrantedRoles[targetRoleArn] {
			if gerr := ensureLambdaPassRoleGrantForJob(ctx, job, targetRoleArn, roleCreated, roleManagedPolicyArns); gerr == nil {
				job.AutoGrantedRoles[targetRoleArn] = true
				_, err = job.DstCli.CreateFunction(ctx, createInput)
			} else {
				return lambdaCopyProgressMsg{Job: job, Name: name, Err: fmt.Errorf("auto-grant failed: %w", gerr), Action: action}
			}
		}
		if err != nil && isLambdaRoleCannotBeAssumedErr(err) {
			if roleCreated {
				time.Sleep(4 * time.Second)
				_, err = job.DstCli.CreateFunction(ctx, createInput)
			} else {
				if altRoleArn, altManaged, aerr := createAlternativeDestinationLambdaRole(ctx, job.Src, job.Dst, *cfg.Role, name); aerr == nil {
					targetRoleArn = altRoleArn
					roleCreated = true
					roleManagedPolicyArns = altManaged
					createInput, _ = buildCreateLambdaInput(name, cfg, srcFn.Code, targetRoleArn)
					_, err = job.DstCli.CreateFunction(ctx, createInput)
				}
			}
			if err != nil && isLambdaPassRoleDeniedErr(err) && !job.AutoGrantedRoles[targetRoleArn] {
				if gerr := ensureLambdaPassRoleGrantForJob(ctx, job, targetRoleArn, roleCreated, roleManagedPolicyArns); gerr == nil {
					job.AutoGrantedRoles[targetRoleArn] = true
					_, err = job.DstCli.CreateFunction(ctx, createInput)
				}
			}
		}
		if err != nil {
			return lambdaCopyProgressMsg{Job: job, Name: name, Err: fmt.Errorf("create: %w", err), Action: action}
		}
		return lambdaCopyProgressMsg{Job: job, Name: name, Action: action}
	}
}

func selectedBucketNames(p panelState) []string {
	out := make([]string, 0, len(p.Marked))
	for _, it := range p.Resources {
		if it.Kind == "bucket" && p.Marked[it.Value] {
			out = append(out, it.Value)
		}
	}
	return out
}

func (m *model) startS3CopyOrMove(move bool) tea.Cmd {
	src := m.activePanel()
	dst := m.getPanel(opposite(m.active))

	objects := selectedObjectKeys(src)
	buckets := selectedBucketNames(src)
	cur, ok := selectedItem(src)
	if len(objects) == 0 && len(buckets) == 0 && ok {
		if cur.Kind == "object" {
			objects = []string{cur.Value}
		} else if cur.Kind == "bucket" {
			buckets = []string{cur.Value}
		} else if cur.Kind == "prefix" {
			keys, err := listAllObjectKeysUnderPrefix(context.Background(), src.Profile, src.Region, src.S3Bucket, cur.Value)
			if err != nil {
				m.status = "Cannot list prefix objects: " + firstLine(err.Error())
				return nil
			}
			objects = keys
		}
	}

	if len(objects) == 0 && len(buckets) == 0 {
		m.status = "Select object(s) or bucket(s)"
		return nil
	}
	if len(objects) > 0 && len(buckets) > 0 {
		m.status = "Mixed selection not supported (objects+buckets)"
		return nil
	}

	if len(objects) > 0 {
		if src.S3Bucket == "" || dst.S3Bucket == "" {
			m.status = "Open source and destination buckets for object copy/move"
			return nil
		}
		m.status = "Checking existing objects in destination..."
		m.pushLog(fmt.Sprintf("START check copy conflicts %s/%s -> %s/%s (%d)", src.Profile, src.S3Bucket, dst.Profile, dst.S3Bucket, len(objects)))
		return preflightCopyCmd(src, dst, objects, move)
	}

	// Bucket operations
	if len(buckets) > 1 && dst.S3Bucket == "" {
		m.status = "When destination is bucket list, select one source bucket"
		return nil
	}

	// If inside destination bucket, copy/move all objects directly there.
	if dst.S3Bucket != "" {
		if len(buckets) == 1 {
			srcBucket := buckets[0]
			m.status = fmt.Sprintf("Preparing object list from %s ...", srcBucket)
			m.pushLog(fmt.Sprintf("START list source bucket for %s :: %s/%s", map[bool]string{true: "move", false: "copy"}[move], src.Profile, srcBucket))
			return prepareBucketCopyJobCmd(src.Profile, src.Region, srcBucket, dst, move)
		}
		if move {
			return moveBucketObjectsCmd(src, dst, buckets)
		}
		return copyBucketObjectsCmd(src, dst, buckets)
	}

	// Destination is bucket list: ask new bucket name (single source only).
	srcBucket := buckets[0]
	defaultName := srcBucket + "-copy"
	target := "copy_bucket_new"
	if move {
		defaultName = srcBucket + "-moved"
		target = "move_bucket_new"
	}
	m.mode = modeInput
	extra := strings.Join([]string{srcBucket, dst.Profile, dst.Region}, "|")
	m.input = newInputState(
		"Destination bucket name",
		fmt.Sprintf("New bucket for %s", srcBucket),
		target,
		m.active,
		defaultName,
		extra,
	)
	return nil
}

func prepareBucketCopyJobCmd(srcProfile, srcRegion, srcBucket string, dst panelState, move bool) tea.Cmd {
	return func() tea.Msg {
		modeLabel := "copy"
		if move {
			modeLabel = "move"
		}
		action := fmt.Sprintf("prepare %s bucket %s/%s -> %s/%s", modeLabel, srcProfile, srcBucket, dst.Profile, dst.S3Bucket)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		keys, err := listAllObjectKeys(ctx, srcProfile, srcRegion, srcBucket)
		return bucketCopyPreparedMsg{
			SrcBucket: srcBucket,
			Dst:       dst,
			Move:      move,
			Keys:      keys,
			Err:       err,
			Action:    action,
		}
	}
}

func preflightCopyCmd(src, dst panelState, keys []string, move bool) tea.Cmd {
	return func() tea.Msg {
		modeLabel := "copy"
		if move {
			modeLabel = "move"
		}
		action := fmt.Sprintf("check %s conflicts %s/%s -> %s/%s", modeLabel, src.Profile, src.S3Bucket, dst.Profile, dst.S3Bucket)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		existing, err := existingDestinationKeys(ctx, dst.Profile, dst.Region, dst.S3Bucket, keys)
		return copyPreflightMsg{
			Src:          src,
			Dst:          dst,
			Keys:         keys,
			Move:         move,
			ExistingKeys: existing,
			Err:          err,
			Action:       action,
		}
	}
}

func preflightLambdaCopyCmd(src, dst panelState, names []string) tea.Cmd {
	return func() tea.Msg {
		action := fmt.Sprintf("check lambda copy conflicts %s/%s -> %s/%s", src.Profile, src.Region, dst.Profile, dst.Region)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		existing, err := existingLambdaNames(ctx, dst.Profile, dst.Region, names)
		return lambdaCopyPreflightMsg{
			Src:      src,
			Dst:      dst,
			Names:    names,
			Existing: existing,
			Err:      err,
			Action:   action,
		}
	}
}

func existingLambdaNames(ctx context.Context, profile, region string, names []string) (map[string]bool, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(resolveProfileCase(profile)),
		config.WithRegion(region),
	)
	if err != nil {
		return nil, err
	}
	cli := lambda.NewFromConfig(cfg)
	out := map[string]bool{}
	for _, name := range names {
		_, err := cli.GetFunction(ctx, &lambda.GetFunctionInput{FunctionName: aws.String(name)})
		if err == nil {
			out[name] = true
			continue
		}
		if isLambdaNotFoundErr(err) {
			continue
		}
		return nil, err
	}
	return out, nil
}

func syncLambdaCode(ctx context.Context, dstCli *lambda.Client, name string, code *lambdatypes.FunctionCodeLocation, arch []lambdatypes.Architecture) error {
	if code == nil {
		return fmt.Errorf("source code metadata is empty")
	}
	if code.ImageUri != nil && strings.TrimSpace(*code.ImageUri) != "" {
		_, err := dstCli.UpdateFunctionCode(ctx, &lambda.UpdateFunctionCodeInput{
			FunctionName:  aws.String(name),
			ImageUri:      code.ImageUri,
			Architectures: arch,
		})
		return err
	}
	if code.Location == nil || strings.TrimSpace(*code.Location) == "" {
		return fmt.Errorf("zip code location is empty")
	}
	zipBytes, err := downloadURL(ctx, *code.Location)
	if err != nil {
		return err
	}
	_, err = dstCli.UpdateFunctionCode(ctx, &lambda.UpdateFunctionCodeInput{
		FunctionName:  aws.String(name),
		ZipFile:       zipBytes,
		Architectures: arch,
	})
	return err
}

func buildCreateLambdaInput(name string, cfg *lambdatypes.FunctionConfiguration, code *lambdatypes.FunctionCodeLocation, roleArn string) (*lambda.CreateFunctionInput, error) {
	var env *lambdatypes.Environment
	if cfg.Environment != nil && len(cfg.Environment.Variables) > 0 {
		env = &lambdatypes.Environment{Variables: cfg.Environment.Variables}
	}
	layers := make([]string, 0, len(cfg.Layers))
	for _, l := range cfg.Layers {
		if l.Arn != nil && strings.TrimSpace(*l.Arn) != "" {
			layers = append(layers, *l.Arn)
		}
	}
	in := &lambda.CreateFunctionInput{
		FunctionName:      aws.String(name),
		Description:       cfg.Description,
		Timeout:           cfg.Timeout,
		MemorySize:        cfg.MemorySize,
		Environment:       env,
		KMSKeyArn:         cfg.KMSKeyArn,
		Layers:            layers,
		FileSystemConfigs: cfg.FileSystemConfigs,
		Architectures:     cfg.Architectures,
		EphemeralStorage:  cfg.EphemeralStorage,
		Role:              aws.String(roleArn),
		PackageType:       cfg.PackageType,
	}
	if cfg.TracingConfig != nil {
		in.TracingConfig = &lambdatypes.TracingConfig{Mode: cfg.TracingConfig.Mode}
	}
	if cfg.DeadLetterConfig != nil {
		in.DeadLetterConfig = cfg.DeadLetterConfig
	}
	if cfg.VpcConfig != nil {
		in.VpcConfig = &lambdatypes.VpcConfig{
			SecurityGroupIds: cfg.VpcConfig.SecurityGroupIds,
			SubnetIds:        cfg.VpcConfig.SubnetIds,
		}
	}
	if cfg.PackageType == lambdatypes.PackageTypeImage {
		if code == nil || code.ImageUri == nil || strings.TrimSpace(*code.ImageUri) == "" {
			return nil, fmt.Errorf("missing image uri")
		}
		in.Code = &lambdatypes.FunctionCode{ImageUri: code.ImageUri}
	} else {
		in.Runtime = cfg.Runtime
		in.Handler = cfg.Handler
		if code == nil || code.Location == nil || strings.TrimSpace(*code.Location) == "" {
			return nil, fmt.Errorf("missing zip code location")
		}
		zipBytes, err := downloadURL(context.Background(), *code.Location)
		if err != nil {
			return nil, err
		}
		in.Code = &lambdatypes.FunctionCode{ZipFile: zipBytes}
	}
	return in, nil
}

func buildUpdateLambdaConfigInput(name string, cfg *lambdatypes.FunctionConfiguration, roleArn string) *lambda.UpdateFunctionConfigurationInput {
	var env *lambdatypes.Environment
	if cfg.Environment != nil {
		env = &lambdatypes.Environment{Variables: cfg.Environment.Variables}
	}
	layers := make([]string, 0, len(cfg.Layers))
	for _, l := range cfg.Layers {
		if l.Arn != nil && strings.TrimSpace(*l.Arn) != "" {
			layers = append(layers, *l.Arn)
		}
	}
	in := &lambda.UpdateFunctionConfigurationInput{
		FunctionName:      aws.String(name),
		Role:              aws.String(roleArn),
		Handler:           cfg.Handler,
		Description:       cfg.Description,
		Timeout:           cfg.Timeout,
		MemorySize:        cfg.MemorySize,
		Environment:       env,
		Runtime:           cfg.Runtime,
		Layers:            layers,
		KMSKeyArn:         cfg.KMSKeyArn,
		FileSystemConfigs: cfg.FileSystemConfigs,
		EphemeralStorage:  cfg.EphemeralStorage,
	}
	if cfg.DeadLetterConfig != nil {
		in.DeadLetterConfig = cfg.DeadLetterConfig
	}
	if cfg.TracingConfig != nil {
		in.TracingConfig = &lambdatypes.TracingConfig{
			Mode: cfg.TracingConfig.Mode,
		}
	}
	if cfg.VpcConfig != nil {
		in.VpcConfig = &lambdatypes.VpcConfig{
			SecurityGroupIds: cfg.VpcConfig.SecurityGroupIds,
			SubnetIds:        cfg.VpcConfig.SubnetIds,
		}
	}
	return in
}

func downloadURL(ctx context.Context, u string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	client := &http.Client{Timeout: 5 * time.Minute}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("http %d downloading code", resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}

func isLambdaNotFoundErr(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "resourcenotfoundexception") || strings.Contains(s, "function not found") || strings.Contains(s, "statuscode: 404")
}

func isLambdaPassRoleDeniedErr(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "cross-account pass role is not allowed") ||
		(strings.Contains(s, "accessdenied") && strings.Contains(s, "pass role")) ||
		strings.Contains(s, "iam:passrole")
}

func isLambdaRoleCannotBeAssumedErr(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "role defined for the function cannot be assumed by lambda")
}

func selectedOrFocusedLambdaName(src panelState) (string, error) {
	marked := selectedGenericValues(src)
	if len(marked) > 0 {
		return marked[0], nil
	}
	it, ok := selectedItem(src)
	if ok && it.Kind == "generic" {
		return it.Value, nil
	}
	return "", fmt.Errorf("select a lambda first")
}

func ensureLambdaPassRoleGrant(ctx context.Context, dst panelState, roleArn string, roleCreated bool, roleManagedPolicyArns []string) error {
	principalArn, err := callerIdentityARN(ctx, dst.Profile, dst.Region)
	if err != nil {
		return err
	}
	dstCfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(resolveProfileCase(dst.Profile)),
		config.WithRegion(dst.Region),
	)
	if err != nil {
		return err
	}
	iamCli := iam.NewFromConfig(dstCfg)
	pt, pn, err := principalAttachmentTarget(principalArn)
	if err != nil {
		return err
	}
	accountID, err := accountIDFromARN(principalArn)
	if err != nil {
		return err
	}

	now := time.Now().UTC()
	policyName := formatPolicySid(fmt.Sprintf("NCAWSLambdaCopyAccess%s", now.Format("20060102T150405")))
	policyDoc := map[string]any{
		"Version": "2012-10-17",
		"Statement": []map[string]any{
			{
				"Sid":      "PassRoleForLambdaCopy",
				"Effect":   "Allow",
				"Action":   []string{"iam:PassRole"},
				"Resource": roleArn,
			},
			{
				"Sid":    "LambdaWriteForCopy",
				"Effect": "Allow",
				"Action": []string{
					"lambda:CreateFunction",
					"lambda:UpdateFunctionCode",
					"lambda:UpdateFunctionConfiguration",
					"lambda:GetFunction",
				},
				"Resource": fmt.Sprintf("arn:aws:lambda:%s:%s:function:*", dst.Region, accountID),
			},
		},
	}
	docBytes, err := json.Marshal(policyDoc)
	if err != nil {
		return err
	}
	createOut, err := iamCli.CreatePolicy(ctx, &iam.CreatePolicyInput{
		PolicyName:     aws.String(policyName),
		PolicyDocument: aws.String(string(docBytes)),
	})
	if err != nil {
		return err
	}
	if createOut.Policy == nil || createOut.Policy.Arn == nil {
		return fmt.Errorf("policy created without arn")
	}
	policyArn := *createOut.Policy.Arn
	if err := attachPolicyToPrincipal(ctx, iamCli, pt, pn, policyArn); err != nil {
		return err
	}
	rec := policyGrantRecord{
		GrantType:                   "lambda",
		ID:                          policyName,
		CreatedAt:                   now.Format(time.RFC3339),
		SourceProfile:               "",
		SourceRegion:                "",
		DestinationProfile:          dst.Profile,
		DestinationRegion:           dst.Region,
		DestinationPrincipalARN:     principalArn,
		IamPolicyArn:                policyArn,
		IamPolicyName:               policyName,
		LambdaRoleArn:               roleArn,
		LambdaRoleCreated:           roleCreated,
		LambdaRoleManagedPolicyArns: append([]string(nil), roleManagedPolicyArns...),
	}
	return appendPolicyGrantRecord(rec)
}

func ensureLambdaPassRoleGrantForJob(ctx context.Context, job lambdaCopyJobState, roleArn string, roleCreated bool, roleManagedPolicyArns []string) error {
	pt, pn, err := principalAttachmentTarget(job.DstPrincipalArn)
	if err != nil {
		return err
	}
	accountID, err := accountIDFromARN(job.DstPrincipalArn)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	policyName := formatPolicySid(fmt.Sprintf("NCAWSLambdaCopyAccess%s", now.Format("20060102T150405")))
	policyDoc := map[string]any{
		"Version": "2012-10-17",
		"Statement": []map[string]any{
			{
				"Sid":      "PassRoleForLambdaCopy",
				"Effect":   "Allow",
				"Action":   []string{"iam:PassRole"},
				"Resource": roleArn,
			},
			{
				"Sid":    "LambdaWriteForCopy",
				"Effect": "Allow",
				"Action": []string{
					"lambda:CreateFunction",
					"lambda:UpdateFunctionCode",
					"lambda:UpdateFunctionConfiguration",
					"lambda:GetFunction",
				},
				"Resource": fmt.Sprintf("arn:aws:lambda:%s:%s:function:*", job.Dst.Region, accountID),
			},
		},
	}
	docBytes, err := json.Marshal(policyDoc)
	if err != nil {
		return err
	}
	createOut, err := job.DstIamCli.CreatePolicy(ctx, &iam.CreatePolicyInput{
		PolicyName:     aws.String(policyName),
		PolicyDocument: aws.String(string(docBytes)),
	})
	if err != nil {
		return err
	}
	if createOut.Policy == nil || createOut.Policy.Arn == nil {
		return fmt.Errorf("policy created without arn")
	}
	policyArn := *createOut.Policy.Arn
	if err := attachPolicyToPrincipal(ctx, job.DstIamCli, pt, pn, policyArn); err != nil {
		return err
	}
	rec := policyGrantRecord{
		GrantType:                   "lambda",
		ID:                          policyName,
		CreatedAt:                   now.Format(time.RFC3339),
		DestinationProfile:          job.Dst.Profile,
		DestinationRegion:           job.Dst.Region,
		DestinationPrincipalARN:     job.DstPrincipalArn,
		IamPolicyArn:                policyArn,
		IamPolicyName:               policyName,
		LambdaRoleArn:               roleArn,
		LambdaRoleCreated:           roleCreated,
		LambdaRoleManagedPolicyArns: append([]string(nil), roleManagedPolicyArns...),
	}
	return appendPolicyGrantRecord(rec)
}

func resolveOrCreateDestinationLambdaRoleARN(ctx context.Context, src panelState, dst panelState, srcRoleArn string) (string, bool, []string, error) {
	srcAccount, err := accountIDFromARN(srcRoleArn)
	if err != nil {
		return "", false, nil, err
	}
	dstPrincipalArn, err := callerIdentityARN(ctx, dst.Profile, dst.Region)
	if err != nil {
		return "", false, nil, err
	}
	dstAccount, err := accountIDFromARN(dstPrincipalArn)
	if err != nil {
		return "", false, nil, err
	}
	if srcAccount == dstAccount {
		return srcRoleArn, false, nil, nil
	}
	rolePath, err := rolePathFromRoleARN(srcRoleArn)
	if err != nil {
		return "", false, nil, err
	}
	candidate := fmt.Sprintf("arn:aws:iam::%s:role/%s", dstAccount, rolePath)

	dstCfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(resolveProfileCase(dst.Profile)),
		config.WithRegion(dst.Region),
	)
	if err != nil {
		return "", false, nil, err
	}
	iamCli := iam.NewFromConfig(dstCfg)
	roleName := roleNameFromPath(rolePath)
	out, err := iamCli.GetRole(ctx, &iam.GetRoleInput{RoleName: aws.String(roleName)})
	if err == nil && out.Role != nil && out.Role.Arn != nil && strings.TrimSpace(*out.Role.Arn) != "" {
		return *out.Role.Arn, false, nil, nil
	}
	if !isIAMNoSuchEntityErr(err) {
		return "", false, nil, fmt.Errorf("destination role lookup failed: %w", err)
	}
	path, _ := splitRolePathAndName(rolePath)
	trustDoc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}`
	_, err = iamCli.CreateRole(ctx, &iam.CreateRoleInput{
		RoleName:                 aws.String(roleName),
		Path:                     aws.String(path),
		AssumeRolePolicyDocument: aws.String(trustDoc),
		Description:              aws.String("Created by nc-aws for cross-account Lambda copy"),
	})
	if err != nil {
		return "", false, nil, fmt.Errorf("create destination role failed (%s): %w", candidate, err)
	}
	managed := collectSourceAWSManagedRolePolicies(ctx, src, srcRoleArn)
	managed = appendIfMissing(managed, "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole")
	for _, p := range managed {
		_, _ = iamCli.AttachRolePolicy(ctx, &iam.AttachRolePolicyInput{
			RoleName:  aws.String(roleName),
			PolicyArn: aws.String(p),
		})
	}
	_ = appendPolicyGrantRecord(policyGrantRecord{
		GrantType:                   "lambda",
		ID:                          formatPolicySid(fmt.Sprintf("NCAWSRoleCreate%s", time.Now().UTC().Format("20060102T150405"))),
		CreatedAt:                   time.Now().UTC().Format(time.RFC3339),
		DestinationProfile:          dst.Profile,
		DestinationRegion:           dst.Region,
		DestinationPrincipalARN:     dstPrincipalArn,
		LambdaRoleArn:               candidate,
		LambdaRoleCreated:           true,
		LambdaRoleManagedPolicyArns: append([]string(nil), managed...),
	})
	return candidate, true, managed, nil
}

func createAlternativeDestinationLambdaRole(ctx context.Context, src panelState, dst panelState, srcRoleArn, lambdaName string) (string, []string, error) {
	dstPrincipalArn, err := callerIdentityARN(ctx, dst.Profile, dst.Region)
	if err != nil {
		return "", nil, err
	}
	dstAccount, err := accountIDFromARN(dstPrincipalArn)
	if err != nil {
		return "", nil, err
	}
	srcPath, err := rolePathFromRoleARN(srcRoleArn)
	if err != nil {
		return "", nil, err
	}
	baseName := roleNameFromPath(srcPath)
	altRoleName := formatPolicySid(fmt.Sprintf("NCAWS-%s-%s", baseName, lambdaName))
	if len(altRoleName) > 64 {
		altRoleName = altRoleName[:64]
	}
	altRoleArn := fmt.Sprintf("arn:aws:iam::%s:role/%s", dstAccount, altRoleName)

	dstCfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(resolveProfileCase(dst.Profile)),
		config.WithRegion(dst.Region),
	)
	if err != nil {
		return "", nil, err
	}
	iamCli := iam.NewFromConfig(dstCfg)
	_, err = iamCli.GetRole(ctx, &iam.GetRoleInput{RoleName: aws.String(altRoleName)})
	if err == nil {
		managed := collectSourceAWSManagedRolePolicies(ctx, src, srcRoleArn)
		managed = appendIfMissing(managed, "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole")
		return altRoleArn, managed, nil
	}
	trustDoc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}`
	_, err = iamCli.CreateRole(ctx, &iam.CreateRoleInput{
		RoleName:                 aws.String(altRoleName),
		Path:                     aws.String("/"),
		AssumeRolePolicyDocument: aws.String(trustDoc),
		Description:              aws.String("Created by nc-aws as fallback role for Lambda copy"),
	})
	if err != nil {
		return "", nil, err
	}
	managed := collectSourceAWSManagedRolePolicies(ctx, src, srcRoleArn)
	managed = appendIfMissing(managed, "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole")
	for _, p := range managed {
		_, _ = iamCli.AttachRolePolicy(ctx, &iam.AttachRolePolicyInput{
			RoleName:  aws.String(altRoleName),
			PolicyArn: aws.String(p),
		})
	}
	_ = appendPolicyGrantRecord(policyGrantRecord{
		GrantType:                   "lambda",
		ID:                          formatPolicySid(fmt.Sprintf("NCAWSRoleCreate%s", time.Now().UTC().Format("20060102T150405"))),
		CreatedAt:                   time.Now().UTC().Format(time.RFC3339),
		DestinationProfile:          dst.Profile,
		DestinationRegion:           dst.Region,
		DestinationPrincipalARN:     dstPrincipalArn,
		LambdaRoleArn:               altRoleArn,
		LambdaRoleCreated:           true,
		LambdaRoleManagedPolicyArns: append([]string(nil), managed...),
	})
	time.Sleep(3 * time.Second)
	return altRoleArn, managed, nil
}

func accountIDFromARN(arn string) (string, error) {
	parts := strings.Split(arn, ":")
	if len(parts) < 5 || strings.TrimSpace(parts[4]) == "" {
		return "", fmt.Errorf("invalid arn: %s", arn)
	}
	return parts[4], nil
}

func rolePathFromRoleARN(arn string) (string, error) {
	i := strings.Index(arn, ":role/")
	if i < 0 {
		return "", fmt.Errorf("not a role arn: %s", arn)
	}
	path := strings.TrimSpace(arn[i+len(":role/"):])
	if path == "" {
		return "", fmt.Errorf("invalid role arn: %s", arn)
	}
	return path, nil
}

func roleNameFromPath(path string) string {
	parts := strings.Split(path, "/")
	return parts[len(parts)-1]
}

func splitRolePathAndName(rolePath string) (path string, roleName string) {
	parts := strings.Split(rolePath, "/")
	if len(parts) == 1 {
		return "/", rolePath
	}
	roleName = parts[len(parts)-1]
	p := strings.Join(parts[:len(parts)-1], "/")
	p = strings.Trim(p, "/")
	if p == "" {
		return "/", roleName
	}
	return "/" + p + "/", roleName
}

func collectSourceAWSManagedRolePolicies(ctx context.Context, src panelState, srcRoleArn string) []string {
	rolePath, err := rolePathFromRoleARN(srcRoleArn)
	if err != nil {
		return nil
	}
	roleName := roleNameFromPath(rolePath)
	srcCfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(resolveProfileCase(src.Profile)),
		config.WithRegion(src.Region),
	)
	if err != nil {
		return nil
	}
	iamCli := iam.NewFromConfig(srcCfg)
	out, err := iamCli.ListAttachedRolePolicies(ctx, &iam.ListAttachedRolePoliciesInput{
		RoleName: aws.String(roleName),
	})
	if err != nil {
		return nil
	}
	policies := make([]string, 0, len(out.AttachedPolicies))
	for _, ap := range out.AttachedPolicies {
		if ap.PolicyArn == nil {
			continue
		}
		arn := strings.TrimSpace(*ap.PolicyArn)
		if strings.HasPrefix(arn, "arn:aws:iam::aws:policy/") {
			policies = appendIfMissing(policies, arn)
		}
	}
	return policies
}

func appendIfMissing(list []string, v string) []string {
	for _, cur := range list {
		if cur == v {
			return list
		}
	}
	return append(list, v)
}

func principalAttachmentTarget(principalArn string) (principalType, principalName string, err error) {
	if i := strings.Index(principalArn, ":user/"); i >= 0 {
		path := principalArn[i+len(":user/"):]
		return "user", roleNameFromPath(path), nil
	}
	if i := strings.Index(principalArn, ":role/"); i >= 0 {
		path := principalArn[i+len(":role/"):]
		return "role", roleNameFromPath(path), nil
	}
	return "", "", fmt.Errorf("unsupported principal type for policy attach: %s", principalArn)
}

func attachPolicyToPrincipal(ctx context.Context, iamCli *iam.Client, principalType, principalName, policyArn string) error {
	switch principalType {
	case "user":
		_, err := iamCli.AttachUserPolicy(ctx, &iam.AttachUserPolicyInput{
			UserName:  aws.String(principalName),
			PolicyArn: aws.String(policyArn),
		})
		return err
	case "role":
		_, err := iamCli.AttachRolePolicy(ctx, &iam.AttachRolePolicyInput{
			RoleName:  aws.String(principalName),
			PolicyArn: aws.String(policyArn),
		})
		return err
	default:
		return fmt.Errorf("unsupported principal type: %s", principalType)
	}
}

func detachPolicyFromPrincipal(ctx context.Context, iamCli *iam.Client, principalType, principalName, policyArn string) error {
	switch principalType {
	case "user":
		_, err := iamCli.DetachUserPolicy(ctx, &iam.DetachUserPolicyInput{
			UserName:  aws.String(principalName),
			PolicyArn: aws.String(policyArn),
		})
		return err
	case "role":
		_, err := iamCli.DetachRolePolicy(ctx, &iam.DetachRolePolicyInput{
			RoleName:  aws.String(principalName),
			PolicyArn: aws.String(policyArn),
		})
		return err
	default:
		return fmt.Errorf("unsupported principal type: %s", principalType)
	}
}

func deleteRoleCompletely(ctx context.Context, iamCli *iam.Client, roleArn string, managedPolicyArns []string) error {
	rolePath, err := rolePathFromRoleARN(roleArn)
	if err != nil {
		return err
	}
	roleName := roleNameFromPath(rolePath)
	for _, p := range managedPolicyArns {
		_, _ = iamCli.DetachRolePolicy(ctx, &iam.DetachRolePolicyInput{
			RoleName:  aws.String(roleName),
			PolicyArn: aws.String(p),
		})
	}
	attached, err := iamCli.ListAttachedRolePolicies(ctx, &iam.ListAttachedRolePoliciesInput{RoleName: aws.String(roleName)})
	if err == nil {
		for _, ap := range attached.AttachedPolicies {
			if ap.PolicyArn == nil {
				continue
			}
			_, _ = iamCli.DetachRolePolicy(ctx, &iam.DetachRolePolicyInput{
				RoleName:  aws.String(roleName),
				PolicyArn: ap.PolicyArn,
			})
		}
	}
	inlineNames, err := iamCli.ListRolePolicies(ctx, &iam.ListRolePoliciesInput{RoleName: aws.String(roleName)})
	if err == nil {
		for _, pn := range inlineNames.PolicyNames {
			name := pn
			_, _ = iamCli.DeleteRolePolicy(ctx, &iam.DeleteRolePolicyInput{
				RoleName:   aws.String(roleName),
				PolicyName: aws.String(name),
			})
		}
	}
	_, err = iamCli.DeleteRole(ctx, &iam.DeleteRoleInput{RoleName: aws.String(roleName)})
	if isIAMNoSuchEntityErr(err) {
		return nil
	}
	return err
}

func isIAMNoSuchEntityErr(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "nosuchentity") || strings.Contains(s, "not found") || strings.Contains(s, "statuscode: 404")
}

func existingDestinationKeys(ctx context.Context, profile, region, bucket string, keys []string) (map[string]bool, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(resolveProfileCase(profile)),
		config.WithRegion(region),
	)
	if err != nil {
		return nil, err
	}
	cli := s3.NewFromConfig(cfg)
	existing := map[string]bool{}
	var mu sync.Mutex
	var firstErr error
	const workers = 8
	jobs := make(chan string)
	var wg sync.WaitGroup

	worker := func() {
		defer wg.Done()
		for key := range jobs {
			mu.Lock()
			stop := firstErr != nil
			mu.Unlock()
			if stop {
				continue
			}
			_, err := cli.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
			})
			if err == nil {
				mu.Lock()
				existing[key] = true
				mu.Unlock()
				continue
			}
			if isS3NotFoundErr(err) {
				continue
			}
			mu.Lock()
			if firstErr == nil {
				firstErr = err
			}
			mu.Unlock()
		}
	}

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go worker()
	}
	for _, key := range keys {
		jobs <- key
	}
	close(jobs)
	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}
	return existing, nil
}

func isS3NotFoundErr(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "notfound") || strings.Contains(s, "no such key") || strings.Contains(s, "statuscode: 404")
}

func isRetryableCopyErr(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "context deadline exceeded") ||
		strings.Contains(s, "timeout") ||
		strings.Contains(s, "connection reset") ||
		strings.Contains(s, "temporarily unavailable")
}

func shortKey(s string, maxLen int) string {
	if maxLen <= 3 || len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}

func firstLine(s string) string {
	if s == "" {
		return ""
	}
	parts := strings.SplitN(s, "\n", 2)
	return parts[0]
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func parentPrefix(prefix string) string {
	if prefix == "" {
		return ""
	}
	trim := strings.TrimSuffix(prefix, "/")
	idx := strings.LastIndex(trim, "/")
	if idx < 0 {
		return ""
	}
	return trim[:idx+1]
}

func parseBucketInputExtra(extra string) (srcBucket, dstProfile, dstRegion string, err error) {
	parts := strings.Split(extra, "|")
	if len(parts) != 3 {
		return "", "", "", fmt.Errorf("invalid operation context")
	}
	srcBucket = strings.TrimSpace(parts[0])
	dstProfile = strings.TrimSpace(parts[1])
	dstRegion = strings.TrimSpace(parts[2])
	if srcBucket == "" || dstProfile == "" || dstRegion == "" {
		return "", "", "", fmt.Errorf("invalid operation context")
	}
	return srcBucket, dstProfile, dstRegion, nil
}

func encodeCopySource(bucket, key string) string {
	parts := strings.Split(key, "/")
	for i, p := range parts {
		parts[i] = url.PathEscape(p)
	}
	return bucket + "/" + strings.Join(parts, "/")
}

func resolveProfileCase(in string) string {
	want := strings.ToLower(strings.TrimSpace(in))
	for _, p := range listProfiles() {
		if strings.ToLower(p) == want {
			return p
		}
	}
	return in
}

func regionsForProfile(profile string) []string {
	pref := profileDefaultRegion(profile)
	out := []string{pref}
	for _, r := range defaultRegions {
		if r != pref {
			out = append(out, r)
		}
	}
	return out
}

func profileDefaultRegion(profile string) string {
	path := awsConfigPath("config")
	f, err := os.Open(path)
	if err != nil {
		return "us-east-1"
	}
	defer f.Close()

	want := strings.ToLower(strings.TrimSpace(profile))
	current := ""
	s := bufio.NewScanner(f)
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			name := strings.TrimSuffix(strings.TrimPrefix(line, "["), "]")
			name = strings.TrimSpace(strings.TrimPrefix(name, "profile "))
			current = strings.ToLower(name)
			continue
		}
		if current == want && strings.HasPrefix(line, "region") {
			parts := strings.SplitN(line, "=", 2)
			if len(parts) == 2 {
				v := strings.TrimSpace(parts[1])
				if v != "" {
					return v
				}
			}
		}
	}
	return "us-east-1"
}

func listProfiles() []string {
	profiles := map[string]struct{}{}
	for _, file := range []string{"config", "credentials"} {
		collectProfileSections(awsConfigPath(file), profiles)
	}
	if len(profiles) == 0 {
		return nil
	}
	out := make([]string, 0, len(profiles))
	for p := range profiles {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}

func profileLabelWithAccount(ctx context.Context, profile string) string {
	key := strings.ToLower(strings.TrimSpace(profile))
	profileAccountCache.RLock()
	if acct, ok := profileAccountCache.values[key]; ok && acct != "" {
		profileAccountCache.RUnlock()
		return fmt.Sprintf("%s (%s)", profile, acct)
	}
	profileAccountCache.RUnlock()

	localCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	accountID, err := callerIdentityAccount(localCtx, profile, profileDefaultRegion(profile))
	if err != nil || strings.TrimSpace(accountID) == "" {
		return profile
	}
	profileAccountCache.Lock()
	profileAccountCache.values[key] = accountID
	profileAccountCache.Unlock()
	return fmt.Sprintf("%s (%s)", profile, accountID)
}

func collectProfileSections(path string, dst map[string]struct{}) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()

	s := bufio.NewScanner(f)
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if !(strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]")) {
			continue
		}
		name := strings.TrimSuffix(strings.TrimPrefix(line, "["), "]")
		name = strings.TrimSpace(name)
		name = strings.TrimPrefix(name, "profile ")
		if name != "" {
			dst[name] = struct{}{}
		}
	}
}

func awsConfigPath(file string) string {
	home, _ := os.UserHomeDir()
	if home == "" {
		home = "."
	}
	return filepath.Join(home, ".aws", file)
}

func callerIdentityARN(ctx context.Context, profile, region string) (string, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(resolveProfileCase(profile)),
		config.WithRegion(region),
	)
	if err != nil {
		return "", err
	}
	out, err := sts.NewFromConfig(cfg).GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return "", err
	}
	return aws.ToString(out.Arn), nil
}

func callerIdentityAccount(ctx context.Context, profile, region string) (string, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(resolveProfileCase(profile)),
		config.WithRegion(region),
	)
	if err != nil {
		return "", err
	}
	out, err := sts.NewFromConfig(cfg).GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return "", err
	}
	return aws.ToString(out.Account), nil
}

func readBucketPolicyDocument(ctx context.Context, cli *s3.Client, bucket string) (map[string]any, error) {
	out, err := cli.GetBucketPolicy(ctx, &s3.GetBucketPolicyInput{Bucket: aws.String(bucket)})
	if err != nil {
		lower := strings.ToLower(err.Error())
		if strings.Contains(lower, "nosuchbucketpolicy") || strings.Contains(lower, "not found") || strings.Contains(lower, "no bucket policy") {
			return map[string]any{"Version": "2012-10-17", "Statement": []any{}}, nil
		}
		return nil, err
	}
	doc := map[string]any{}
	if err := json.Unmarshal([]byte(aws.ToString(out.Policy)), &doc); err != nil {
		return nil, err
	}
	if _, ok := doc["Version"]; !ok {
		doc["Version"] = "2012-10-17"
	}
	if _, ok := doc["Statement"]; !ok {
		doc["Statement"] = []any{}
	}
	return doc, nil
}

func putBucketPolicyDocument(ctx context.Context, cli *s3.Client, bucket string, doc map[string]any) error {
	b, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	_, err = cli.PutBucketPolicy(ctx, &s3.PutBucketPolicyInput{
		Bucket: aws.String(bucket),
		Policy: aws.String(string(b)),
	})
	return err
}

func addPolicyStatement(doc map[string]any, st map[string]any) {
	raw, ok := doc["Statement"]
	if !ok {
		doc["Statement"] = []any{st}
		return
	}
	switch v := raw.(type) {
	case []any:
		doc["Statement"] = append(v, st)
	case map[string]any:
		doc["Statement"] = []any{v, st}
	default:
		doc["Statement"] = []any{st}
	}
}

func removePolicyStatementBySid(doc map[string]any, sid string) {
	raw, ok := doc["Statement"]
	if !ok {
		return
	}
	var in []any
	switch v := raw.(type) {
	case []any:
		in = v
	case map[string]any:
		in = []any{v}
	default:
		return
	}
	out := make([]any, 0, len(in))
	for _, x := range in {
		m, ok := x.(map[string]any)
		if !ok {
			out = append(out, x)
			continue
		}
		curSid, _ := m["Sid"].(string)
		if curSid == sid {
			continue
		}
		out = append(out, m)
	}
	doc["Statement"] = out
}

func formatPolicySid(s string) string {
	var b strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		}
	}
	out := b.String()
	if out == "" {
		out = "NCAWSPolicy"
	}
	if len(out) > 100 {
		out = out[:100]
	}
	return out
}

func loadPolicyRegistry() (policyRegistry, error) {
	var reg policyRegistry
	b, err := os.ReadFile(policyRegistryPath())
	if err != nil {
		if os.IsNotExist(err) {
			return policyRegistry{}, nil
		}
		return reg, err
	}
	if len(b) == 0 {
		return policyRegistry{}, nil
	}
	if err := json.Unmarshal(b, &reg); err != nil {
		return reg, err
	}
	return reg, nil
}

func savePolicyRegistry(reg policyRegistry) error {
	b, err := json.MarshalIndent(reg, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(policyRegistryPath(), b, 0o600)
}

func appendPolicyGrantRecord(rec policyGrantRecord) error {
	reg, err := loadPolicyRegistry()
	if err != nil {
		return err
	}
	reg.Records = append(reg.Records, rec)
	return savePolicyRegistry(reg)
}

func (m *model) reloadGrantRegistry() {
	items, err := listActiveGrantRecords()
	if err != nil {
		m.status = "Load grants registry error: " + firstLine(err.Error())
		m.lastInfo = err.Error()
		m.pushLog("ERROR load grants registry :: " + err.Error())
		m.grants = grantRegistryState{}
		return
	}
	cur := m.grants.Cursor
	if cur >= len(items) {
		cur = max(0, len(items)-1)
	}
	m.grants = grantRegistryState{Items: items, Cursor: cur}
}

func listActiveGrantRecords() ([]policyGrantRecord, error) {
	reg, err := loadPolicyRegistry()
	if err != nil {
		return nil, err
	}
	out := make([]policyGrantRecord, 0, len(reg.Records))
	for i := len(reg.Records) - 1; i >= 0; i-- {
		r := reg.Records[i]
		if r.Revoked {
			continue
		}
		if normalizedGrantType(r) != "s3" && normalizedGrantType(r) != "lambda" {
			continue
		}
		out = append(out, r)
	}
	return out, nil
}

func grantRecordLabel(r policyGrantRecord) string {
	gt := normalizedGrantType(r)
	if gt == "s3" {
		return fmt.Sprintf("s3 | id=%s | bucket=%s | dst=%s/%s", r.ID, r.SourceBucket, r.DestinationProfile, r.DestinationRegion)
	}
	role := r.LambdaRoleArn
	if role == "" {
		role = "-"
	}
	return fmt.Sprintf("lambda | id=%s | dst=%s/%s | role=%s", r.ID, r.DestinationProfile, r.DestinationRegion, role)
}

func normalizedGrantType(r policyGrantRecord) string {
	t := strings.ToLower(strings.TrimSpace(r.GrantType))
	if t == "" {
		return "s3"
	}
	return t
}

func findLatestActiveGrantIndex(reg policyRegistry, bucket, principalArn string) int {
	for i := len(reg.Records) - 1; i >= 0; i-- {
		r := reg.Records[i]
		if r.Revoked {
			continue
		}
		if r.GrantType != "" && r.GrantType != "s3" {
			continue
		}
		if r.SourceBucket == bucket && strings.EqualFold(r.DestinationPrincipalARN, principalArn) {
			return i
		}
	}
	return -1
}

func findActiveGrantRecordIndex(reg policyRegistry, rec policyGrantRecord) int {
	for i := len(reg.Records) - 1; i >= 0; i-- {
		r := reg.Records[i]
		if r.Revoked {
			continue
		}
		if r.ID != rec.ID {
			continue
		}
		if normalizedGrantType(r) != normalizedGrantType(rec) {
			continue
		}
		return i
	}
	return -1
}

func findLatestActiveLambdaGrantIndex(reg policyRegistry, dstProfile, dstRegion, principalArn, roleArn string) int {
	for i := len(reg.Records) - 1; i >= 0; i-- {
		r := reg.Records[i]
		if r.Revoked || r.GrantType != "lambda" {
			continue
		}
		if !strings.EqualFold(r.DestinationProfile, dstProfile) || !strings.EqualFold(r.DestinationRegion, dstRegion) {
			continue
		}
		if !strings.EqualFold(r.DestinationPrincipalARN, principalArn) {
			continue
		}
		if !strings.EqualFold(r.LambdaRoleArn, roleArn) {
			continue
		}
		return i
	}
	return -1
}

func policyRegistryPath() string {
	home, _ := os.UserHomeDir()
	if home == "" {
		home = "."
	}
	return filepath.Join(home, policyRegistryFileName)
}

func revokeGrantRecordCmd(rec policyGrantRecord) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		if err := revokeGrantRecord(ctx, rec); err != nil {
			return grantRevokeMsg{Err: err, GrantID: rec.ID}
		}
		return grantRevokeMsg{Status: fmt.Sprintf("Grant revoked (%s)", rec.ID), GrantID: rec.ID}
	}
}

func revokeGrantRecord(ctx context.Context, rec policyGrantRecord) error {
	reg, err := loadPolicyRegistry()
	if err != nil {
		return err
	}
	idx := findActiveGrantRecordIndex(reg, rec)
	if idx < 0 {
		return fmt.Errorf("grant not found or already revoked: %s", rec.ID)
	}
	r := reg.Records[idx]
	switch normalizedGrantType(r) {
	case "s3":
		srcCfg, err := config.LoadDefaultConfig(ctx,
			config.WithSharedConfigProfile(resolveProfileCase(r.SourceProfile)),
			config.WithRegion(r.SourceRegion),
		)
		if err != nil {
			return err
		}
		s3cli := s3.NewFromConfig(srcCfg)
		doc, err := readBucketPolicyDocument(ctx, s3cli, r.SourceBucket)
		if err != nil {
			return err
		}
		removePolicyStatementBySid(doc, r.SidGetObject)
		removePolicyStatementBySid(doc, r.SidListBucket)
		if err := putBucketPolicyDocument(ctx, s3cli, r.SourceBucket, doc); err != nil {
			return err
		}
	case "lambda":
		dstCfg, err := config.LoadDefaultConfig(ctx,
			config.WithSharedConfigProfile(resolveProfileCase(r.DestinationProfile)),
			config.WithRegion(r.DestinationRegion),
		)
		if err != nil {
			return err
		}
		iamCli := iam.NewFromConfig(dstCfg)
		if r.DestinationPrincipalARN != "" && r.IamPolicyArn != "" {
			pt, pn, err := principalAttachmentTarget(r.DestinationPrincipalARN)
			if err == nil {
				if err := detachPolicyFromPrincipal(ctx, iamCli, pt, pn, r.IamPolicyArn); err != nil && !isIAMNoSuchEntityErr(err) {
					return err
				}
			}
			if _, err := iamCli.DeletePolicy(ctx, &iam.DeletePolicyInput{PolicyArn: aws.String(r.IamPolicyArn)}); err != nil && !isIAMNoSuchEntityErr(err) {
				return err
			}
		}
		if r.LambdaRoleCreated && r.LambdaRoleArn != "" {
			if err := deleteRoleCompletely(ctx, iamCli, r.LambdaRoleArn, r.LambdaRoleManagedPolicyArns); err != nil && !isIAMNoSuchEntityErr(err) {
				return err
			}
		}
	default:
		return fmt.Errorf("unsupported grant type: %s", r.GrantType)
	}
	reg.Records[idx].Revoked = true
	reg.Records[idx].RevokedAt = time.Now().UTC().Format(time.RFC3339)
	return savePolicyRegistry(reg)
}

func describePanelLoad(p panelState) string {
	switch p.Level {
	case levelProfiles:
		return "list profiles"
	case levelRegions:
		return fmt.Sprintf("list regions profile=%s", p.Profile)
	case levelServices:
		return fmt.Sprintf("list services profile=%s region=%s", p.Profile, p.Region)
	case levelResources:
		if p.Service == "s3" {
			if p.S3Bucket == "" {
				return fmt.Sprintf("list s3 buckets profile=%s region=%s", p.Profile, p.Region)
			}
			return fmt.Sprintf("list s3 objects profile=%s region=%s bucket=%s prefix=%s", p.Profile, p.Region, p.S3Bucket, p.S3Prefix)
		}
		return fmt.Sprintf("list %s resources profile=%s region=%s", p.Service, p.Profile, p.Region)
	default:
		return "list resources"
	}
}

func describeDetail(p panelState, it resourceItem) string {
	if p.Service == "s3" && p.S3Bucket != "" && it.Kind == "object" {
		return fmt.Sprintf("head s3 object profile=%s region=%s bucket=%s key=%s", p.Profile, p.Region, p.S3Bucket, it.Value)
	}
	return fmt.Sprintf("detail service=%s value=%s", p.Service, it.Value)
}

func (m *model) pushLog(line string) {
	if strings.TrimSpace(line) == "" {
		return
	}
	entry := fmt.Sprintf("%s | %s", time.Now().Format("15:04:05"), line)
	m.logLines = append(m.logLines, entry)
	const keep = 120
	if len(m.logLines) > keep {
		m.logLines = m.logLines[len(m.logLines)-keep:]
	}
}

func suggestFixForError(action string, err error) string {
	if err == nil {
		return ""
	}
	e := strings.ToLower(err.Error())
	a := strings.ToLower(action)

	// Most common case in this app: cross-account S3 copy without read grants on source.
	if (strings.Contains(a, "copy s3") || strings.Contains(a, "move s3")) &&
		(strings.Contains(e, "accessdenied") || strings.Contains(e, "forbidden") || strings.Contains(e, "statuscode: 403")) {
		return "S3 access denied detected. If this is cross-account copy/move, press 'a' to create source-bucket read policy for destination principal, then retry 'c'/'m'. Use 'u' to revoke later."
	}

	if strings.Contains(a, "copy lambda") && isLambdaPassRoleDeniedErr(err) {
		return "Lambda pass-role denied. The app can auto-create a destination IAM policy and attach it for copy. Retry copy; or press 'a' in Lambda view to grant now, and 'u' to revoke later."
	}

	if strings.Contains(e, "nosuchbucket") {
		return "Bucket not found. Verify source/destination bucket names and the selected profile/region in each panel."
	}

	if strings.Contains(e, "invalidaccesskeyid") || strings.Contains(e, "signaturedoesnotmatch") {
		return "Credentials/signature issue. Verify selected profile credentials and AWS region."
	}

	if strings.Contains(e, "throttl") || strings.Contains(e, "slowdown") {
		return "AWS throttling detected. Wait a few seconds and retry."
	}

	return ""
}

func (m *model) persistContext() {
	if err := saveSessionState(*m); err != nil {
		m.pushLog("ERROR save session :: " + err.Error())
	}
}

func applyPersistedSession(m *model, s persistedSessionState) {
	if s.Active != leftPanel && s.Active != rightPanel {
		m.active = leftPanel
	} else {
		m.active = s.Active
	}
	m.left = fromPersistedPanel(s.Left)
	m.right = fromPersistedPanel(s.Right)
}

func fromPersistedPanel(p persistedPanelState) panelState {
	level := p.Level
	if level < levelProfiles || level > levelResources {
		level = levelProfiles
	}
	return panelState{
		Level:    level,
		Profile:  p.Profile,
		Region:   p.Region,
		Service:  p.Service,
		S3Bucket: p.S3Bucket,
		S3Prefix: p.S3Prefix,
		Cursor:   max(0, p.Cursor),
		Loading:  true,
		Marked:   map[string]bool{},
	}
}

func toPersistedPanel(p panelState) persistedPanelState {
	return persistedPanelState{
		Level:    p.Level,
		Profile:  p.Profile,
		Region:   p.Region,
		Service:  p.Service,
		S3Bucket: p.S3Bucket,
		S3Prefix: p.S3Prefix,
		Cursor:   p.Cursor,
	}
}

func saveSessionState(m model) error {
	state := persistedSessionState{
		Active: m.active,
		Left:   toPersistedPanel(m.left),
		Right:  toPersistedPanel(m.right),
	}
	b, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(sessionStatePath(), b, 0o600)
}

func loadSessionState() (persistedSessionState, error) {
	var s persistedSessionState
	b, err := os.ReadFile(sessionStatePath())
	if err != nil {
		return s, err
	}
	if err := json.Unmarshal(b, &s); err != nil {
		return s, err
	}
	return s, nil
}

func sessionStatePath() string {
	home, _ := os.UserHomeDir()
	if home == "" {
		home = "."
	}
	return filepath.Join(home, sessionFileName)
}
