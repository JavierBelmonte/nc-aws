package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/apigatewayv2"
	apigwtypes "github.com/aws/aws-sdk-go-v2/service/apigatewayv2/types"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cloudformationtypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/ecr"
	ecrtypes "github.com/aws/aws-sdk-go-v2/service/ecr/types"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	ecstypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	kmstypes "github.com/aws/aws-sdk-go-v2/service/kms/types"
	awsrds "github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
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
	modeApigwConflict
	modeEcsConflict
	modeEcsSvcConflict
	modeEcrConflict
	modeSecretsConflict
	modeRdsCopyConfirm
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

type apigwConflictState struct {
	Title    string
	Src      panelState
	Dst      panelState
	ApiIds   []string
	Existing map[string]bool
}

type apigwCopyJobState struct {
	Src          panelState
	Dst          panelState
	ApiIds       []string
	Index        int
	Success      int
	Failed       int
	Skipped      int
	SkipExisting bool
	Existing     map[string]bool
	SrcCli       *apigatewayv2.Client
	DstCli       *apigatewayv2.Client
}

type apigwCopyProgressMsg struct {
	Job     apigwCopyJobState
	ApiName string
	Err     error
	Skipped bool
	Action  string
}

type apigwCopyPreflightMsg struct {
	Src      panelState
	Dst      panelState
	ApiIds   []string
	Existing map[string]bool
	Err      error
	Action   string
}

type ecsConflictState struct {
	Title        string
	Src          panelState
	Dst          panelState
	Clusters     []string
	Existing     map[string]bool
}

type ecsCopyJobState struct {
	Src          panelState
	Dst          panelState
	Clusters     []string
	Index        int
	Success      int
	Failed       int
	Skipped      int
	SkipExisting bool
	Existing     map[string]bool
	SrcCli       *ecs.Client
	DstCli       *ecs.Client
}

type ecsCopyProgressMsg struct {
	Job         ecsCopyJobState
	ClusterName string
	Err         error
	Skipped     bool
	Action      string
}

type ecsCopyPreflightMsg struct {
	Src      panelState
	Dst      panelState
	Clusters []string
	Existing map[string]bool
	Err      error
	Action   string
}

type ecrConflictState struct {
	Title    string
	Src      panelState
	Dst      panelState
	Repos    []string
	Existing map[string]bool
}

type ecrCopyJobState struct {
	Src          panelState
	Dst          panelState
	Repos        []string
	Index        int
	Success      int
	Failed       int
	Skipped      int
	SkipExisting bool
	Existing     map[string]bool
	SrcCli       *ecr.Client
	DstCli       *ecr.Client
	// per-repo image tracking:
	Images      []ecrtypes.ImageIdentifier
	ImageIndex  int
	ImageErrors int
}

// ecrCopyProgressMsg signals repo-level completion (skip or prep error).
type ecrCopyProgressMsg struct {
	Job      ecrCopyJobState
	RepoName string
	Err      error
	Skipped  bool
	Action   string
}

// ecrRepoReadyMsg is sent once a destination repo is prepared and image list fetched.
type ecrRepoReadyMsg struct {
	Job    ecrCopyJobState
	Images []ecrtypes.ImageIdentifier
	Action string
	Err    error
}

// ecrImageProgressMsg is sent after copying one image (layers + manifest).
type ecrImageProgressMsg struct {
	Job      ecrCopyJobState
	RepoName string
	ImageTag string
	Layers   int
	Err      error
	Action   string
}

type ecrCopyPreflightMsg struct {
	Src      panelState
	Dst      panelState
	Repos    []string
	Existing map[string]bool
	Err      error
	Action   string
}

type secretsConflictState struct {
	Title    string
	Src      panelState
	Dst      panelState
	Names    []string
	Existing map[string]bool
}

type secretsCopyJobState struct {
	Src          panelState
	Dst          panelState
	Names        []string
	Index        int
	Success      int
	Failed       int
	Skipped      int
	SkipExisting bool
	Existing     map[string]bool
	SrcCli       *secretsmanager.Client
	DstCli       *secretsmanager.Client
}

type secretsCopyProgressMsg struct {
	Job        secretsCopyJobState
	SecretName string
	Err        error
	Skipped    bool
	Action     string
}

type secretsCopyPreflightMsg struct {
	Src      panelState
	Dst      panelState
	Names    []string
	Existing map[string]bool
	Err      error
	Action   string
}

type rdsCredentials struct {
	Username string
	Password string
	Engine   string
	Endpoint string
	Port     string
}

type pendingRdsCredsState struct {
	PanelID    panelID
	Profile    string
	Region     string
	InstanceID string
	Engine     string
	Endpoint   string
	Port       string
	Username   string
}

type pendingRdsCreateState struct {
	PanelID    panelID
	Identifier string
	Engine     string
	Class      string
	Username   string
}

type rdsCopyConfirmState struct {
	Src        panelState
	Dst        panelState
	InstanceID string
	Engine     string
}

type rdsInstanceInfoMsg struct {
	PanelID    panelID
	InstanceID string
	Engine     string
	Endpoint   string
	Port       string
	Err        error
}

type rdsConnectedMsg struct {
	PanelID    panelID
	Profile    string
	Region     string
	InstanceID string
	Items      []resourceItem
	Err        error
}

type rdsDbConnectedMsg struct {
	PanelID    panelID
	InstanceID string
	DbName     string
	Items      []resourceItem
	Err        error
}

type rdsCopyJobState struct {
	Src           panelState
	Dst           panelState
	InstanceID    string
	NewInstanceID string
	SnapshotID    string
	CopiedSnapID  string
	DstSnapArn    string
	Step          string // "init" | "wait_src" | "wait_dst" | "restore"
	PollCount     int
	StartedAt     time.Time
	StepStartAt   time.Time
	DstAccountID  string
	SrcCli          *awsrds.Client
	DstCli          *awsrds.Client
	// KmsKeyID holds the CMK ARN used when the original snapshot was aws/rds encrypted
	// and needed re-encryption before cross-account sharing.
	KmsKeyID        string
	ReencryptSnapID string // non-empty when a re-encryption step was needed
}

type rdsCopyProgressMsg struct {
	Job    rdsCopyJobState
	Status string
	Err    error
	Done   bool
}

type ecsSvcConflictState struct {
	Title             string
	Src               panelState
	Dst               panelState
	ServiceArns       []string
	Existing          map[string]bool
	DstSubnets        []string
	DstSecurityGroups []string
}

type ecsSvcCopyJobState struct {
	Src               panelState
	Dst               panelState
	ServiceArns       []string
	Index             int
	Success           int
	Failed            int
	Skipped           int
	SkipExisting      bool
	Existing          map[string]bool
	DstSubnets        []string
	DstSecurityGroups []string
	SrcCli            *ecs.Client
	DstCli            *ecs.Client
}

// pendingEcsSvcCopyParams holds the parameters collected during the network
// config input steps before the copy job is started.
type pendingEcsSvcCopyParams struct {
	Src          panelState
	Dst          panelState
	ServiceArns  []string
	SkipExisting bool
	Existing     map[string]bool
	DstSubnets   []string
}

type ecsSvcCopyProgressMsg struct {
	Job         ecsSvcCopyJobState
	ServiceName string
	Err         error
	Skipped     bool
	Action      string
}

type ecsSvcCopyPreflightMsg struct {
	Src                panelState
	Dst                panelState
	ServiceArns        []string
	Existing           map[string]bool
	NeedsNetworkConfig bool
	AutoDstSubnets     []string
	AutoDstSecGroups   []string
	AutoNetworkMsg     string
	Err                error
	Action             string
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
	apigwConf          apigwConflictState
	ecsConf            ecsConflictState
	ecrConf            ecrConflictState
	ecsCopyJob         *ecsCopyJobState
	ecsSvcConf         ecsSvcConflictState
	ecsSvcCopyJob      *ecsSvcCopyJobState
	pendingEcsSvc      *pendingEcsSvcCopyParams
	ecrCopyJob         *ecrCopyJobState
	secretsConf        secretsConflictState
	secretsCopyJob     *secretsCopyJobState
	pendingRdsCreds    *pendingRdsCredsState
	pendingRdsCreate   *pendingRdsCreateState
	rdsCopyConf        rdsCopyConfirmState
	rdsCopyJob         *rdsCopyJobState
	grants             grantRegistryState
	themeIndex         int
	viewText           string
	copyJob            *copyJobState
	lambdaJob          *lambdaCopyJobState
	apigwJob           *apigwCopyJobState
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
	Note    string // optional log line (e.g. role created)
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

type cfDeletePreviewMsg struct {
	PanelID panelID
	Panel   panelState
	Stack   string
	Preview string
	Err     error
	Action  string
}

type cfDeleteAutoRefreshMsg struct {
	Remaining int
}

type uiTheme struct {
	Name           string
	BaseFg         string
	BaseBg         string
	HeaderFg       string
	ShortcutFg     string
	StatusFg       string
	BorderActive   string
	BorderInactive string
	DialogBorder   string
	LogBorder      string
	ErrorFg        string
}

var services = []string{"s3", "lambda", "cloudformation", "ecs", "ecr", "vpc", "apigateway", "secretsmanager", "rds"}
var defaultRegions = []string{"us-east-1", "us-east-2", "us-west-1", "us-west-2", "eu-west-1", "eu-central-1", "sa-east-1"}
var uiThemes = []uiTheme{
	{Name: "NC Classic", BaseFg: "15", BaseBg: "18", HeaderFg: "15", ShortcutFg: "153", StatusFg: "123", BorderActive: "15", BorderInactive: "27", DialogBorder: "15", LogBorder: "75", ErrorFg: "203"},
	{Name: "Emerald Terminal", BaseFg: "231", BaseBg: "22", HeaderFg: "121", ShortcutFg: "157", StatusFg: "120", BorderActive: "120", BorderInactive: "29", DialogBorder: "121", LogBorder: "35", ErrorFg: "210"},
	{Name: "Amber Phosphor", BaseFg: "230", BaseBg: "52", HeaderFg: "220", ShortcutFg: "223", StatusFg: "214", BorderActive: "214", BorderInactive: "94", DialogBorder: "220", LogBorder: "136", ErrorFg: "209"},
	{Name: "Iceberg", BaseFg: "254", BaseBg: "17", HeaderFg: "117", ShortcutFg: "153", StatusFg: "159", BorderActive: "117", BorderInactive: "24", DialogBorder: "111", LogBorder: "67", ErrorFg: "217"},
	{Name: "Graphite", BaseFg: "252", BaseBg: "235", HeaderFg: "81", ShortcutFg: "246", StatusFg: "121", BorderActive: "81", BorderInactive: "240", DialogBorder: "186", LogBorder: "244", ErrorFg: "203"},
	{Name: "Solar Sand", BaseFg: "16", BaseBg: "230", HeaderFg: "24", ShortcutFg: "60", StatusFg: "28", BorderActive: "31", BorderInactive: "137", DialogBorder: "94", LogBorder: "101", ErrorFg: "124"},
	{Name: "Crimson Night", BaseFg: "255", BaseBg: "52", HeaderFg: "210", ShortcutFg: "217", StatusFg: "186", BorderActive: "203", BorderInactive: "88", DialogBorder: "210", LogBorder: "131", ErrorFg: "224"},
	{Name: "Aqua Ops", BaseFg: "231", BaseBg: "23", HeaderFg: "51", ShortcutFg: "159", StatusFg: "122", BorderActive: "45", BorderInactive: "30", DialogBorder: "51", LogBorder: "37", ErrorFg: "203"},
	{Name: "Plum Paper", BaseFg: "255", BaseBg: "90", HeaderFg: "225", ShortcutFg: "218", StatusFg: "223", BorderActive: "225", BorderInactive: "97", DialogBorder: "219", LogBorder: "140", ErrorFg: "224"},
	{Name: "Mono Slate", BaseFg: "255", BaseBg: "236", HeaderFg: "255", ShortcutFg: "250", StatusFg: "153", BorderActive: "255", BorderInactive: "244", DialogBorder: "252", LogBorder: "246", ErrorFg: "210"},
}
var profileAccountCache = struct {
	sync.RWMutex
	values map[string]string
}{values: map[string]string{}}

var rdsConnCache = struct {
	sync.RWMutex
	conns map[string]*sql.DB
	creds map[string]rdsCredentials
}{
	conns: map[string]*sql.DB{},
	creds: map[string]rdsCredentials{},
}

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
		cmds := []tea.Cmd{loadPanelCmd(leftPanel, m.left), loadPanelCmd(rightPanel, m.right)}
		if strings.HasPrefix(strings.ToLower(msg.Action), "delete cloudformation stack ") {
			cmds = append(cmds, scheduleCFDeleteAutoRefreshCmd(4))
		}
		return m, tea.Batch(cmds...)
	case cfDeleteAutoRefreshMsg:
		cmds := []tea.Cmd{loadPanelCmd(leftPanel, m.left), loadPanelCmd(rightPanel, m.right)}
		if msg.Remaining > 1 {
			cmds = append(cmds, scheduleCFDeleteAutoRefreshCmd(msg.Remaining-1))
		}
		return m, tea.Batch(cmds...)
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
		if msg.Note != "" {
			m.pushLog(fmt.Sprintf("INFO %s :: lambda=%s :: %s", msg.Action, msg.Name, msg.Note))
		}
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
	case apigwCopyPreflightMsg:
		if msg.Err != nil {
			m.status = "Preflight error: " + firstLine(msg.Err.Error())
			m.lastInfo = msg.Err.Error()
			m.pushLog(fmt.Sprintf("ERROR %s :: %s", msg.Action, msg.Err.Error()))
			return m, nil
		}
		conflicts := len(msg.Existing)
		m.pushLog(fmt.Sprintf("OK %s :: %d conflict(s)", msg.Action, conflicts))
		if conflicts == 0 {
			return m, m.startApigwCopyJob(msg.Src, msg.Dst, msg.ApiIds, false, msg.Existing)
		}
		m.apigwConf = apigwConflictState{
			Title:    fmt.Sprintf("Found %d existing API(s) in destination. [o] overwrite all  [s] skip existing  [esc] cancel", conflicts),
			Src:      msg.Src,
			Dst:      msg.Dst,
			ApiIds:   msg.ApiIds,
			Existing: msg.Existing,
		}
		m.mode = modeApigwConflict
		m.status = fmt.Sprintf("Conflicts detected: %d existing API(s)", conflicts)
		return m, nil
	case apigwCopyProgressMsg:
		job := msg.Job
		if msg.Err != nil {
			job.Failed++
			m.pushLog(fmt.Sprintf("ERROR %s :: api=%s :: %s", msg.Action, msg.ApiName, msg.Err.Error()))
		} else if msg.Skipped {
			job.Skipped++
			m.pushLog(fmt.Sprintf("SKIP %s :: api=%s", msg.Action, msg.ApiName))
		} else {
			job.Success++
			m.pushLog(fmt.Sprintf("OK %s :: api=%s", msg.Action, msg.ApiName))
		}
		job.Index++
		m.apigwJob = &job
		if job.Index >= len(job.ApiIds) {
			if job.Failed > 0 {
				m.status = fmt.Sprintf("API Gateway copy finished with errors: ok=%d skip=%d fail=%d", job.Success, job.Skipped, job.Failed)
			} else {
				m.status = fmt.Sprintf("API Gateway copy completed: ok=%d skip=%d", job.Success, job.Skipped)
			}
			m.lastInfo = m.status
			m.apigwJob = nil
			m.mode = modeNormal
			m.persistContext()
			return m, tea.Batch(loadPanelCmd(leftPanel, m.left), loadPanelCmd(rightPanel, m.right))
		}
		m.status = fmt.Sprintf("Copying API Gateway %d/%d", job.Index+1, len(job.ApiIds))
		return m, apigwCopyStepCmd(job)
	case ecsCopyPreflightMsg:
		if msg.Err != nil {
			m.status = "Preflight error: " + firstLine(msg.Err.Error())
			m.lastInfo = msg.Err.Error()
			m.pushLog(fmt.Sprintf("ERROR %s :: %s", msg.Action, msg.Err.Error()))
			return m, nil
		}
		conflicts := len(msg.Existing)
		m.pushLog(fmt.Sprintf("OK %s :: %d conflict(s)", msg.Action, conflicts))
		if conflicts == 0 {
			return m, m.startEcsCopyJob(msg.Src, msg.Dst, msg.Clusters, false, msg.Existing)
		}
		m.ecsConf = ecsConflictState{
			Title:    fmt.Sprintf("Found %d existing cluster(s) in destination. [o] overwrite  [s] skip existing  [esc] cancel", conflicts),
			Src:      msg.Src,
			Dst:      msg.Dst,
			Clusters: msg.Clusters,
			Existing: msg.Existing,
		}
		m.mode = modeEcsConflict
		m.status = fmt.Sprintf("Conflicts detected: %d existing ECS cluster(s)", conflicts)
		return m, nil
	case ecsCopyProgressMsg:
		job := msg.Job
		if msg.Err != nil {
			job.Failed++
			m.pushLog(fmt.Sprintf("ERROR %s :: cluster=%s :: %s", msg.Action, msg.ClusterName, msg.Err.Error()))
		} else if msg.Skipped {
			job.Skipped++
			m.pushLog(fmt.Sprintf("SKIP %s :: cluster=%s", msg.Action, msg.ClusterName))
		} else {
			job.Success++
			m.pushLog(fmt.Sprintf("OK %s :: cluster=%s", msg.Action, msg.ClusterName))
		}
		job.Index++
		m.ecsCopyJob = &job
		if job.Index >= len(job.Clusters) {
			if job.Failed > 0 {
				m.status = fmt.Sprintf("ECS copy finished with errors: ok=%d skip=%d fail=%d", job.Success, job.Skipped, job.Failed)
			} else {
				m.status = fmt.Sprintf("ECS copy completed: ok=%d skip=%d", job.Success, job.Skipped)
			}
			m.lastInfo = m.status
			m.ecsCopyJob = nil
			m.mode = modeNormal
			m.persistContext()
			return m, tea.Batch(loadPanelCmd(leftPanel, m.left), loadPanelCmd(rightPanel, m.right))
		}
		m.status = fmt.Sprintf("Copying ECS cluster %d/%d :: %s", job.Index+1, len(job.Clusters), shortKey(job.Clusters[job.Index], 72))
		return m, ecsCopyStepCmd(job)
	case ecsSvcCopyPreflightMsg:
		if msg.Err != nil {
			m.status = "Preflight error: " + firstLine(msg.Err.Error())
			m.lastInfo = msg.Err.Error()
			m.pushLog(fmt.Sprintf("ERROR %s :: %s", msg.Action, msg.Err.Error()))
			return m, nil
		}
		conflicts := len(msg.Existing)
		m.pushLog(fmt.Sprintf("OK %s :: %d conflict(s)", msg.Action, conflicts))
		if msg.AutoNetworkMsg != "" {
			m.pushLog("AUTO-DETECT network: " + msg.AutoNetworkMsg)
		}
		if msg.NeedsNetworkConfig {
			// Cross-account/region FARGATE services: auto-detection failed, ask user.
			m.pendingEcsSvc = &pendingEcsSvcCopyParams{
				Src:          msg.Src,
				Dst:          msg.Dst,
				ServiceArns:  msg.ServiceArns,
				SkipExisting: false,
				Existing:     msg.Existing,
			}
			m.mode = modeInput
			m.input = newInputState(
				"ECS Service Network Config",
				"Destination subnet IDs (comma-separated):",
				"ecs_svc_copy_subnets", m.active, "", "")
			m.status = "Enter destination subnet IDs for FARGATE services (auto-detect failed)"
			return m, nil
		}
		// Use auto-detected subnets/SGs if available.
		dstSubnets := msg.AutoDstSubnets
		dstSGs := msg.AutoDstSecGroups
		if conflicts == 0 {
			return m, m.startEcsSvcCopyJob(msg.Src, msg.Dst, msg.ServiceArns, false, msg.Existing, dstSubnets, dstSGs)
		}
		m.ecsSvcConf = ecsSvcConflictState{
			Title:             fmt.Sprintf("Found %d existing service(s) in destination cluster. [o] overwrite  [s] skip existing  [esc] cancel", conflicts),
			Src:               msg.Src,
			Dst:               msg.Dst,
			ServiceArns:       msg.ServiceArns,
			Existing:          msg.Existing,
			DstSubnets:        dstSubnets,
			DstSecurityGroups: dstSGs,
		}
		m.mode = modeEcsSvcConflict
		m.status = fmt.Sprintf("Conflicts detected: %d existing ECS service(s)", conflicts)
		return m, nil
	case ecsSvcCopyProgressMsg:
		job := msg.Job
		if msg.Err != nil {
			job.Failed++
			m.pushLog(fmt.Sprintf("ERROR %s :: service=%s :: %s", msg.Action, msg.ServiceName, msg.Err.Error()))
		} else if msg.Skipped {
			job.Skipped++
			m.pushLog(fmt.Sprintf("SKIP %s :: service=%s", msg.Action, msg.ServiceName))
		} else {
			job.Success++
			m.pushLog(fmt.Sprintf("OK %s :: service=%s", msg.Action, msg.ServiceName))
		}
		job.Index++
		m.ecsSvcCopyJob = &job
		if job.Index >= len(job.ServiceArns) {
			if job.Failed > 0 {
				m.status = fmt.Sprintf("ECS service copy finished with errors: ok=%d skip=%d fail=%d", job.Success, job.Skipped, job.Failed)
			} else {
				m.status = fmt.Sprintf("ECS service copy completed: ok=%d skip=%d", job.Success, job.Skipped)
			}
			m.lastInfo = m.status
			m.ecsSvcCopyJob = nil
			m.mode = modeNormal
			m.persistContext()
			return m, tea.Batch(loadPanelCmd(leftPanel, m.left), loadPanelCmd(rightPanel, m.right))
		}
		m.status = fmt.Sprintf("Copying ECS service %d/%d :: %s", job.Index+1, len(job.ServiceArns), shortKey(job.ServiceArns[job.Index], 72))
		return m, ecsSvcCopyStepCmd(job)
	case ecrCopyPreflightMsg:
		if msg.Err != nil {
			m.status = "Preflight error: " + firstLine(msg.Err.Error())
			m.lastInfo = msg.Err.Error()
			m.pushLog(fmt.Sprintf("ERROR %s :: %s", msg.Action, msg.Err.Error()))
			return m, nil
		}
		conflicts := len(msg.Existing)
		m.pushLog(fmt.Sprintf("OK %s :: %d conflict(s)", msg.Action, conflicts))
		if conflicts == 0 {
			return m, m.startEcrCopyJob(msg.Src, msg.Dst, msg.Repos, false, msg.Existing)
		}
		m.ecrConf = ecrConflictState{
			Title:    fmt.Sprintf("Found %d existing repo(s) in destination. [o] overwrite images  [s] skip existing repos  [esc] cancel", conflicts),
			Src:      msg.Src,
			Dst:      msg.Dst,
			Repos:    msg.Repos,
			Existing: msg.Existing,
		}
		m.mode = modeEcrConflict
		m.status = fmt.Sprintf("Conflicts detected: %d existing ECR repo(s)", conflicts)
		return m, nil
	case ecrCopyProgressMsg:
		// Repo-level result: skip or prep error.
		job := msg.Job
		if msg.Err != nil {
			job.Failed++
			m.pushLog(fmt.Sprintf("ERROR %s :: repo=%s :: %s", msg.Action, msg.RepoName, msg.Err.Error()))
		} else if msg.Skipped {
			job.Skipped++
			m.pushLog(fmt.Sprintf("SKIP %s :: repo=%s", msg.Action, msg.RepoName))
		} else {
			job.Success++
			m.pushLog(fmt.Sprintf("OK %s :: repo=%s", msg.Action, msg.RepoName))
		}
		job.Index++
		if job.Index >= len(job.Repos) {
			return m, m.ecrAllDone(job)
		}
		m.ecrCopyJob = &job
		m.status = fmt.Sprintf("ECR: preparing repo %s (%d/%d)...", job.Repos[job.Index], job.Index+1, len(job.Repos))
		return m, ecrPrepRepoCmd(job)
	case ecrRepoReadyMsg:
		job := msg.Job
		if msg.Err != nil {
			job.Failed++
			m.pushLog(fmt.Sprintf("ERROR ecr prep repo :: repo=%s :: %s", job.Repos[job.Index], msg.Err.Error()))
			job.Index++
			if job.Index >= len(job.Repos) {
				return m, m.ecrAllDone(job)
			}
			m.ecrCopyJob = &job
			m.status = fmt.Sprintf("ECR: preparing repo %s (%d/%d)...", job.Repos[job.Index], job.Index+1, len(job.Repos))
			return m, ecrPrepRepoCmd(job)
		}
		job.Images = msg.Images
		job.ImageIndex = 0
		job.ImageErrors = 0
		m.ecrCopyJob = &job
		repoName := job.Repos[job.Index]
		if len(job.Images) == 0 {
			m.pushLog(fmt.Sprintf("OK ecr repo %s :: 0 images, nothing to copy", repoName))
			job.Success++
			job.Index++
			if job.Index >= len(job.Repos) {
				return m, m.ecrAllDone(job)
			}
			m.status = fmt.Sprintf("ECR: preparing repo %s (%d/%d)...", job.Repos[job.Index], job.Index+1, len(job.Repos))
			return m, ecrPrepRepoCmd(job)
		}
		m.status = fmt.Sprintf("ECR %s [1/%d]: starting...", repoName, len(job.Images))
		return m, ecrCopyImageCmd(job)
	case ecrImageProgressMsg:
		job := msg.Job
		repoName := job.Repos[job.Index]
		if msg.Err != nil {
			job.ImageErrors++
			m.pushLog(fmt.Sprintf("ERROR ecr copy image :: repo=%s tag=%s :: %s", repoName, msg.ImageTag, msg.Err.Error()))
		} else {
			m.pushLog(fmt.Sprintf("OK ecr copy image :: repo=%s tag=%s layers=%d", repoName, msg.ImageTag, msg.Layers))
		}
		job.ImageIndex++
		if job.ImageIndex < len(job.Images) {
			m.ecrCopyJob = &job
			m.status = fmt.Sprintf("ECR %s [%d/%d]: %s (%d layers copied)", repoName, job.ImageIndex+1, len(job.Images), msg.ImageTag, msg.Layers)
			return m, ecrCopyImageCmd(job)
		}
		// All images in this repo done.
		if job.ImageErrors > 0 {
			job.Failed++
			m.pushLog(fmt.Sprintf("FAIL ecr repo %s :: %d image error(s) out of %d", repoName, job.ImageErrors, len(job.Images)))
		} else {
			job.Success++
			m.pushLog(fmt.Sprintf("OK ecr repo %s :: %d images copied", repoName, len(job.Images)))
		}
		job.Index++
		if job.Index >= len(job.Repos) {
			return m, m.ecrAllDone(job)
		}
		m.ecrCopyJob = &job
		m.status = fmt.Sprintf("ECR: preparing repo %s (%d/%d)...", job.Repos[job.Index], job.Index+1, len(job.Repos))
		return m, ecrPrepRepoCmd(job)
	case secretsCopyPreflightMsg:
		if msg.Err != nil {
			m.status = "Preflight error: " + firstLine(msg.Err.Error())
			m.lastInfo = msg.Err.Error()
			m.pushLog(fmt.Sprintf("ERROR %s :: %s", msg.Action, msg.Err.Error()))
			return m, nil
		}
		conflicts := len(msg.Existing)
		m.pushLog(fmt.Sprintf("OK %s :: %d conflict(s)", msg.Action, conflicts))
		if conflicts == 0 {
			return m, m.startSecretsCopyJob(msg.Src, msg.Dst, msg.Names, false, msg.Existing)
		}
		m.secretsConf = secretsConflictState{
			Title:    fmt.Sprintf("Found %d existing secret(s) in destination. [o] overwrite all  [s] skip existing  [esc] cancel", conflicts),
			Src:      msg.Src,
			Dst:      msg.Dst,
			Names:    msg.Names,
			Existing: msg.Existing,
		}
		m.mode = modeSecretsConflict
		m.status = fmt.Sprintf("Conflicts detected: %d existing secret(s)", conflicts)
		return m, nil
	case secretsCopyProgressMsg:
		job := msg.Job
		if msg.Err != nil {
			job.Failed++
			m.pushLog(fmt.Sprintf("ERROR %s :: secret=%s :: %s", msg.Action, msg.SecretName, msg.Err.Error()))
		} else if msg.Skipped {
			job.Skipped++
			m.pushLog(fmt.Sprintf("SKIP %s :: secret=%s", msg.Action, msg.SecretName))
		} else {
			job.Success++
			m.pushLog(fmt.Sprintf("OK %s :: secret=%s", msg.Action, msg.SecretName))
		}
		job.Index++
		m.secretsCopyJob = &job
		if job.Index >= len(job.Names) {
			status := fmt.Sprintf("Secrets copy completed: ok=%d skip=%d fail=%d", job.Success, job.Skipped, job.Failed)
			m.status = status
			m.lastInfo = status
			m.secretsCopyJob = nil
			m.mode = modeNormal
			m.persistContext()
			return m, tea.Batch(loadPanelCmd(leftPanel, m.left), loadPanelCmd(rightPanel, m.right))
		}
		m.status = fmt.Sprintf("Copying secret %d/%d :: %s", job.Index+1, len(job.Names), job.Names[job.Index])
		return m, secretsCopyStepCmd(job)
	case rdsInstanceInfoMsg:
		if msg.Err != nil {
			m.status = "RDS error: " + firstLine(msg.Err.Error())
			m.lastInfo = msg.Err.Error()
			m.pushLog(fmt.Sprintf("ERROR rds describe instance %s :: %s", msg.InstanceID, msg.Err.Error()))
			return m, nil
		}
		m.pendingRdsCreds = &pendingRdsCredsState{
			PanelID:    msg.PanelID,
			Profile:    m.getPanel(msg.PanelID).Profile,
			Region:     m.getPanel(msg.PanelID).Region,
			InstanceID: msg.InstanceID,
			Engine:     msg.Engine,
			Endpoint:   msg.Endpoint,
			Port:       msg.Port,
		}
		m.active = msg.PanelID
		m.mode = modeInput
		m.input = newInputState(
			fmt.Sprintf("Connect to RDS: %s (%s)", msg.InstanceID, msg.Engine),
			"Username:",
			"rds_creds_user",
			msg.PanelID, "", "")
		m.status = "Enter RDS username"
		return m, nil
	case rdsConnectedMsg:
		if msg.Err != nil {
			m.status = "RDS connect error: " + firstLine(msg.Err.Error())
			m.lastInfo = msg.Err.Error()
			m.pushLog(fmt.Sprintf("ERROR rds connect %s :: %s", msg.InstanceID, msg.Err.Error()))
			return m, nil
		}
		p := m.getPanel(msg.PanelID)
		p.S3Bucket = msg.InstanceID
		p.S3Prefix = ""
		p.Resources = msg.Items
		p.Cursor = 0
		p.Marked = map[string]bool{}
		m.setPanel(msg.PanelID, p)
		m.persistContext()
		m.status = fmt.Sprintf("Connected to %s — %d database(s)", msg.InstanceID, len(msg.Items))
		m.pushLog(fmt.Sprintf("OK rds connected %s :: %d dbs", msg.InstanceID, len(msg.Items)))
		return m, nil
	case rdsDbConnectedMsg:
		if msg.Err != nil {
			m.status = "RDS DB error: " + firstLine(msg.Err.Error())
			m.lastInfo = msg.Err.Error()
			m.pushLog(fmt.Sprintf("ERROR rds connect db %s/%s :: %s", msg.InstanceID, msg.DbName, msg.Err.Error()))
			return m, nil
		}
		p := m.getPanel(m.active)
		p.S3Prefix = msg.DbName
		p.Resources = msg.Items
		p.Cursor = 0
		p.Marked = map[string]bool{}
		m.setPanel(m.active, p)
		m.persistContext()
		m.status = fmt.Sprintf("DB %s — %d table(s)", msg.DbName, len(msg.Items))
		m.pushLog(fmt.Sprintf("OK rds db %s/%s :: %d tables", msg.InstanceID, msg.DbName, len(msg.Items)))
		return m, nil
	case rdsCopyProgressMsg:
		if msg.Err != nil {
			m.status = "RDS copy error: " + firstLine(msg.Err.Error())
			m.lastInfo = msg.Err.Error()
			m.pushLog(fmt.Sprintf("ERROR rds copy %s :: %s", msg.Job.InstanceID, msg.Err.Error()))
			m.rdsCopyJob = nil
			return m, tea.Batch(loadPanelCmd(leftPanel, m.left), loadPanelCmd(rightPanel, m.right))
		}
		if msg.Done {
			m.status = msg.Status
			m.lastInfo = msg.Status
			m.pushLog("OK rds copy :: " + msg.Status)
			m.rdsCopyJob = nil
			m.persistContext()
			return m, tea.Batch(loadPanelCmd(leftPanel, m.left), loadPanelCmd(rightPanel, m.right))
		}
		job := msg.Job
		m.rdsCopyJob = &job
		elapsed := fmtDuration(time.Since(job.StartedAt))
		eta := rdsETAStr(job)
		m.status = fmt.Sprintf("[RDS copy] %s | %s | %s elapsed%s",
			job.InstanceID, rdsStepLabel(job.Step, job.PollCount), elapsed, eta)
		m.pushLog(fmt.Sprintf("INFO rds copy %s :: step=%s poll=%d elapsed=%s",
			job.InstanceID, job.Step, job.PollCount, elapsed))
		return m, rdsCopyStepCmd(job)
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
	case cfDeletePreviewMsg:
		if msg.Err != nil {
			m.status = "CloudFormation preview error: " + firstLine(msg.Err.Error())
			m.lastInfo = msg.Err.Error()
			m.pushLog(fmt.Sprintf("ERROR %s :: %s", msg.Action, msg.Err.Error()))
			return m, nil
		}
		m.active = msg.PanelID
		m.mode = modeConfirm
		m.confirm = confirmState{
			Title:   msg.Preview + "\n\nDelete CloudFormation stack? y/n",
			Target:  "delete_cf_stack",
			PanelID: msg.PanelID,
			Payload: msg.Stack,
		}
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
		if m.secretsCopyJob != nil {
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
		if m.rdsCopyJob != nil {
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
		case modeApigwConflict:
			switch msg.String() {
			case "o":
				m.mode = modeNormal
				return m, m.startApigwCopyJob(m.apigwConf.Src, m.apigwConf.Dst, m.apigwConf.ApiIds, false, m.apigwConf.Existing)
			case "s":
				m.mode = modeNormal
				return m, m.startApigwCopyJob(m.apigwConf.Src, m.apigwConf.Dst, m.apigwConf.ApiIds, true, m.apigwConf.Existing)
			case "esc", "q", "c":
				m.mode = modeNormal
				m.status = "Canceled"
				m.pushLog("OK apigateway conflict resolution :: canceled")
				return m, nil
			}
			return m, nil
		case modeEcsConflict:
			switch msg.String() {
			case "o":
				m.mode = modeNormal
				return m, m.startEcsCopyJob(m.ecsConf.Src, m.ecsConf.Dst, m.ecsConf.Clusters, false, m.ecsConf.Existing)
			case "s":
				m.mode = modeNormal
				return m, m.startEcsCopyJob(m.ecsConf.Src, m.ecsConf.Dst, m.ecsConf.Clusters, true, m.ecsConf.Existing)
			case "esc", "q", "c":
				m.mode = modeNormal
				m.status = "Canceled"
				m.pushLog("OK ecs conflict resolution :: canceled")
				return m, nil
			}
			return m, nil
		case modeEcsSvcConflict:
			switch msg.String() {
			case "o":
				m.mode = modeNormal
				return m, m.startEcsSvcCopyJob(m.ecsSvcConf.Src, m.ecsSvcConf.Dst, m.ecsSvcConf.ServiceArns, false, m.ecsSvcConf.Existing, m.ecsSvcConf.DstSubnets, m.ecsSvcConf.DstSecurityGroups)
			case "s":
				m.mode = modeNormal
				return m, m.startEcsSvcCopyJob(m.ecsSvcConf.Src, m.ecsSvcConf.Dst, m.ecsSvcConf.ServiceArns, true, m.ecsSvcConf.Existing, m.ecsSvcConf.DstSubnets, m.ecsSvcConf.DstSecurityGroups)
			case "esc", "q", "c":
				m.mode = modeNormal
				m.status = "Canceled"
				m.pushLog("OK ecs service conflict resolution :: canceled")
				return m, nil
			}
			return m, nil
		case modeEcrConflict:
			switch msg.String() {
			case "o":
				m.mode = modeNormal
				return m, m.startEcrCopyJob(m.ecrConf.Src, m.ecrConf.Dst, m.ecrConf.Repos, false, m.ecrConf.Existing)
			case "s":
				m.mode = modeNormal
				return m, m.startEcrCopyJob(m.ecrConf.Src, m.ecrConf.Dst, m.ecrConf.Repos, true, m.ecrConf.Existing)
			case "esc", "q", "c":
				m.mode = modeNormal
				m.status = "Canceled"
				m.pushLog("OK ecr conflict resolution :: canceled")
				return m, nil
			}
			return m, nil
		case modeSecretsConflict:
			switch msg.String() {
			case "o":
				m.mode = modeNormal
				return m, m.startSecretsCopyJob(m.secretsConf.Src, m.secretsConf.Dst, m.secretsConf.Names, false, m.secretsConf.Existing)
			case "s":
				m.mode = modeNormal
				return m, m.startSecretsCopyJob(m.secretsConf.Src, m.secretsConf.Dst, m.secretsConf.Names, true, m.secretsConf.Existing)
			case "esc", "q", "c":
				m.mode = modeNormal
				m.status = "Canceled"
				m.pushLog("OK secrets conflict resolution :: canceled")
				return m, nil
			}
			return m, nil
		case modeRdsCopyConfirm:
			switch msg.String() {
			case "w":
				m.mode = modeNormal
				conf := m.rdsCopyConf
				m.status = "Starting RDS snapshot copy..."
				m.pushLog(fmt.Sprintf("START rds copy with data %s -> %s/%s", conf.InstanceID, conf.Dst.Profile, conf.Dst.Region))
				return m, rdsCopyWithDataCmd(conf.Src, conf.Dst, conf.InstanceID)
			case "s":
				m.mode = modeNormal
				conf := m.rdsCopyConf
				m.status = "Starting RDS schema-only copy..."
				m.pushLog(fmt.Sprintf("START rds copy schema only %s -> %s/%s", conf.InstanceID, conf.Dst.Profile, conf.Dst.Region))
				return m, rdsCopySchemaOnlyCmd(conf.Src, conf.Dst, conf.InstanceID)
			case "esc", "q":
				m.mode = modeNormal
				m.status = "Canceled"
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
		case "t":
			m.themeIndex = (m.themeIndex + 1) % len(uiThemes)
			m.status = "Theme: " + currentTheme(m).Name
			m.lastInfo = m.status
			return m, nil
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
			firstPress := prevLast.IsZero() || now.Sub(prevLast) > 800*time.Millisecond
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

			// Hold SPACE ~2s (auto-repeat) to toggle selection of all selectable items.
			if !m.spaceHoldTriggered && now.Sub(m.spaceHoldStart) >= 2*time.Second {
				count := toggleAllSelectableItems(&p)
				m.spaceHoldTriggered = true
				m.setActivePanel(p)
				m.persistContext()
				m.status = fmt.Sprintf("%d item(s) selected", count)
				return m, nil
			}

			// Ignore auto-repeat events while key is held to avoid rapid toggle flicker.
			if !firstPress {
				return m, nil
			}

			markable := false
			if p.Service == "s3" {
				markable = it.Kind == "object" || it.Kind == "bucket"
			} else if p.Service == "rds" {
				markable = it.Kind == "rds-instance" || it.Kind == "rds-cluster"
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
			// RDS instance/cluster: check connection cache before drilling in.
			if p.Level == levelResources && p.Service == "rds" && p.S3Bucket == "" &&
				(it.Kind == "rds-instance" || it.Kind == "rds-cluster") {
				cacheKey := rdsCacheKey(p.Profile, p.Region, it.Value)
				rdsConnCache.RLock()
				_, hasConn := rdsConnCache.conns[cacheKey]
				rdsConnCache.RUnlock()
				if hasConn {
					// Already connected — drill in.
					p.S3Bucket = it.Value
					p.S3Prefix = ""
					p.Cursor = 0
					p.Marked = map[string]bool{}
					m.setActivePanel(p)
					m.persistContext()
					return m, loadPanelCmd(m.active, p)
				}
				// Need credentials — fetch instance info first.
				m.status = "Fetching RDS instance info..."
				return m, rdsGetInstanceInfoCmd(m.active, p, it.Value)
			}
			// RDS db: postgres needs per-db connection.
			if p.Level == levelResources && p.Service == "rds" && p.S3Bucket != "" && p.S3Prefix == "" && it.Kind == "rds-db" {
				cacheKey := rdsCacheKey(p.Profile, p.Region, p.S3Bucket)
				rdsConnCache.RLock()
				creds, hasCreds := rdsConnCache.creds[cacheKey]
				rdsConnCache.RUnlock()
				if hasCreds && isPostgres(creds.Engine) {
					pgKey := cacheKey + ":" + it.Value
					rdsConnCache.RLock()
					_, hasPgConn := rdsConnCache.conns[pgKey]
					rdsConnCache.RUnlock()
					if !hasPgConn {
						m.status = "Connecting to database " + it.Value + "..."
						return m, rdsConnectToDBCmd(m.active, p, creds, it.Value)
					}
					p.S3Prefix = it.Value
					p.Cursor = 0
					m.setActivePanel(p)
					m.persistContext()
					return m, loadPanelCmd(m.active, p)
				}
				// MySQL: just set S3Prefix, same connection works for all DBs.
				p.S3Prefix = it.Value
				p.Cursor = 0
				m.setActivePanel(p)
				m.persistContext()
				return m, loadPanelCmd(m.active, p)
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
			if p.Level == levelResources && p.Service == "s3" && p.S3Bucket == "" {
				m.mode = modeInput
				m.input = newInputState("Create S3 Bucket", "Bucket name", "create_s3_bucket", m.active, "", "")
				return m, nil
			}
			if p.Level == levelResources && p.Service == "ecr" && p.S3Bucket == "" {
				m.mode = modeInput
				m.input = newInputState("Create ECR Repository", "Repository name", "create_ecr_repo", m.active, "", "")
				return m, nil
			}
			if p.Level == levelResources && p.Service == "secretsmanager" {
				m.mode = modeInput
				m.input = newInputState("Create Secret", "Secret name:", "create_secret_name", m.active, "", "")
				return m, nil
			}
			if p.Level == levelResources && p.Service == "rds" && p.S3Bucket == "" {
				m.pendingRdsCreate = &pendingRdsCreateState{PanelID: m.active}
				m.mode = modeInput
				m.input = newInputState("Create RDS Instance", "DB instance identifier:", "rds_create_id", m.active, "", "")
				return m, nil
			}
			m.status = "Create only available in S3 (buckets), ECR (repositories), Secrets Manager, or RDS"
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
			if p.Level == levelResources && p.Service == "cloudformation" && it.Kind == "generic" {
				m.status = "Loading stack resources preview..."
				return m, prepareCFDeletePreviewCmd(m.active, p, it.Value)
			}
			if p.Level == levelResources && p.Service == "ecs" && it.Kind == "generic" {
				if p.S3Bucket != "" {
					// Inside a cluster — deleting a service.
					m.mode = modeConfirm
					m.confirm = confirmState{
						Title:   "Delete ECS service (force)? y/n",
						Target:  "delete_ecs_service",
						PanelID: m.active,
						Payload: it.Value,
					}
				} else {
					m.mode = modeConfirm
					m.confirm = confirmState{Title: "Delete ECS cluster? y/n", Target: "delete_ecs_cluster", PanelID: m.active, Payload: it.Value}
				}
				return m, nil
			}
			if p.Level == levelResources && p.Service == "ecr" && it.Kind == "ecr-image" {
				shortDigest := it.Value
				if len(shortDigest) > 19 {
					shortDigest = shortDigest[:19]
				}
				m.mode = modeConfirm
				m.confirm = confirmState{Title: fmt.Sprintf("Delete image %s? y/n", shortDigest), Target: "delete_ecr_image", PanelID: m.active, Payload: it.Value}
				return m, nil
			}
			if p.Level == levelResources && p.Service == "ecr" && it.Kind == "generic" {
				m.mode = modeConfirm
				m.confirm = confirmState{Title: "Delete ECR repository (force)? y/n", Target: "delete_ecr_repo", PanelID: m.active, Payload: it.Value}
				return m, nil
			}
			if p.Level == levelResources && p.Service == "apigateway" && it.Kind == "generic" {
				m.mode = modeConfirm
				m.confirm = confirmState{Title: "Delete API Gateway API? y/n", Target: "delete_apigateway_api", PanelID: m.active, Payload: it.Value}
				return m, nil
			}
			if p.Level == levelResources && p.Service == "secretsmanager" && it.Kind == "generic" {
				marked := selectedGenericValues(p)
				if len(marked) > 1 {
					m.mode = modeConfirm
					m.confirm = confirmState{
						Title:   fmt.Sprintf("Delete %d secret(s) permanently? y/n", len(marked)),
						Target:  "delete_secret_bulk",
						PanelID: m.active,
						Payload: strings.Join(marked, "\n"),
					}
					return m, nil
				}
				m.mode = modeConfirm
				m.confirm = confirmState{Title: "Delete secret permanently? y/n", Target: "delete_secret", PanelID: m.active, Payload: it.Value}
				return m, nil
			}
			if p.Level == levelResources && p.Service == "rds" && p.S3Bucket == "" &&
				(it.Kind == "rds-instance" || it.Kind == "rds-cluster") {
				marked := selectedRdsValues(p)
				if len(marked) > 1 {
					m.mode = modeConfirm
					m.confirm = confirmState{
						Title:   fmt.Sprintf("Delete %d RDS instance(s)? y/n", len(marked)),
						Target:  "delete_rds_bulk",
						PanelID: m.active,
						Payload: strings.Join(marked, "\n"),
					}
					return m, nil
				}
				m.mode = modeConfirm
				m.confirm = confirmState{Title: fmt.Sprintf("Delete RDS instance %s? y/n", it.Value), Target: "delete_rds_instance", PanelID: m.active, Payload: it.Value}
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
			if src.Level == levelResources && dst.Level == levelResources && src.Service == "apigateway" && dst.Service == "apigateway" {
				ids := selectedGenericValues(src)
				if len(ids) == 0 {
					it, ok := selectedItem(src)
					if ok && it.Kind == "generic" {
						ids = []string{it.Value}
					}
				}
				if len(ids) == 0 {
					m.status = "Select API(s) to copy"
					return m, nil
				}
				m.status = "Checking existing APIs in destination..."
				m.pushLog(fmt.Sprintf("START check apigateway copy conflicts %s/%s -> %s/%s (%d)", src.Profile, src.Region, dst.Profile, dst.Region, len(ids)))
				return m, preflightApigwCopyCmd(src, dst, ids)
			}
			if src.Level == levelResources && dst.Level == levelResources && src.Service == "ecs" && dst.Service == "ecs" && src.S3Bucket == "" {
				arns := selectedGenericValues(src)
				if len(arns) == 0 {
					it, ok := selectedItem(src)
					if ok && it.Kind == "generic" {
						arns = []string{it.Value}
					}
				}
				if len(arns) == 0 {
					m.status = "Select ECS cluster(s) to copy"
					return m, nil
				}
				m.status = "Checking existing clusters in destination..."
				m.pushLog(fmt.Sprintf("START check ecs copy conflicts %s/%s -> %s/%s (%d)", src.Profile, src.Region, dst.Profile, dst.Region, len(arns)))
				return m, preflightEcsCopyCmd(src, dst, arns)
			}
			if src.Level == levelResources && dst.Level == levelResources && src.Service == "ecs" && dst.Service == "ecs" && src.S3Bucket != "" {
				if dst.S3Bucket == "" {
					m.status = "Destination panel must be inside an ECS cluster to copy services"
					return m, nil
				}
				arns := selectedGenericValues(src)
				if len(arns) == 0 {
					it, ok := selectedItem(src)
					if ok && it.Kind == "generic" {
						arns = []string{it.Value}
					}
				}
				if len(arns) == 0 {
					m.status = "Select ECS service(s) to copy"
					return m, nil
				}
				m.status = "Checking existing services in destination cluster..."
				m.pushLog(fmt.Sprintf("START check ecs service copy conflicts %s/%s/%s -> %s/%s/%s (%d)", src.Profile, src.Region, src.S3Bucket, dst.Profile, dst.Region, dst.S3Bucket, len(arns)))
				return m, preflightEcsSvcCopyCmd(src, dst, arns)
			}
			if src.Level == levelResources && dst.Level == levelResources && src.Service == "ecr" && dst.Service == "ecr" && src.S3Bucket == "" {
				repos := selectedGenericValues(src)
				if len(repos) == 0 {
					it, ok := selectedItem(src)
					if ok && it.Kind == "generic" {
						repos = []string{it.Value}
					}
				}
				if len(repos) == 0 {
					m.status = "Select ECR repository/repositories to copy"
					return m, nil
				}
				m.status = "Checking existing repositories in destination..."
				m.pushLog(fmt.Sprintf("START check ecr copy conflicts %s/%s -> %s/%s (%d)", src.Profile, src.Region, dst.Profile, dst.Region, len(repos)))
				return m, preflightEcrCopyCmd(src, dst, repos)
			}
			if src.Level == levelResources && dst.Level == levelResources && src.Service == "secretsmanager" && dst.Service == "secretsmanager" {
				names := selectedGenericValues(src)
				if len(names) == 0 {
					it, ok := selectedItem(src)
					if ok && it.Kind == "generic" {
						names = []string{it.Value}
					}
				}
				if len(names) == 0 {
					m.status = "Select secret(s) to copy"
					return m, nil
				}
				m.status = "Checking existing secrets in destination..."
				m.pushLog(fmt.Sprintf("START check secrets copy conflicts %s/%s -> %s/%s (%d)", src.Profile, src.Region, dst.Profile, dst.Region, len(names)))
				return m, preflightSecretsCopyCmd(src, dst, names)
			}
			if src.Level == levelResources && dst.Level == levelResources &&
				src.Service == "rds" && dst.Service == "rds" && src.S3Bucket == "" {
				it, ok := selectedItem(src)
				if !ok || (it.Kind != "rds-instance" && it.Kind != "rds-cluster") {
					m.status = "Select an RDS instance or cluster to copy"
					return m, nil
				}
				cacheKey := rdsCacheKey(src.Profile, src.Region, it.Value)
				rdsConnCache.RLock()
				creds, hasCreds := rdsConnCache.creds[cacheKey]
				rdsConnCache.RUnlock()
				engine := ""
				if hasCreds {
					engine = creds.Engine
				}
				m.rdsCopyConf = rdsCopyConfirmState{
					Src:        src,
					Dst:        dst,
					InstanceID: it.Value,
					Engine:     engine,
				}
				m.mode = modeRdsCopyConfirm
				m.status = fmt.Sprintf("Copy RDS instance %s to %s/%s", it.Value, dst.Profile, dst.Region)
				return m, nil
			}
			m.status = "Copy requires both panels in S3, Lambda, API Gateway, ECS, ECR, Secrets Manager, or RDS resource view"
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
		if p.Service == "ecs" {
			switch it.Kind {
			case "generic":
				if p.S3Bucket == "" {
					// Enter on a cluster → drill into services.
					p.S3Bucket = it.Value
					p.S3Prefix = ""
					p.Cursor = 0
					p.Marked = map[string]bool{}
					return true, true
				}
			case "back":
				p.S3Bucket = ""
				p.S3Prefix = ""
				p.Cursor = 0
				p.Marked = map[string]bool{}
				return true, true
			}
			return false, false
		}
		if p.Service == "ecr" {
			switch it.Kind {
			case "generic":
				if p.S3Bucket == "" {
					// Enter on a repository → drill into images.
					p.S3Bucket = it.Value
					p.S3Prefix = ""
					p.Cursor = 0
					p.Marked = map[string]bool{}
					return true, true
				}
			case "back":
				p.S3Bucket = ""
				p.S3Prefix = ""
				p.Cursor = 0
				p.Marked = map[string]bool{}
				return true, true
			}
			return false, false
		}
		if p.Service == "rds" {
			switch it.Kind {
			case "rds-db":
				// MySQL path: same connection works, just set S3Prefix.
				p.S3Prefix = it.Value
				p.Cursor = 0
				return true, true
			case "rds-table":
				// View rows — detail handled by detailCmd.
				return false, false
			case "back":
				if p.S3Prefix != "" {
					p.S3Prefix = ""
				} else {
					p.S3Bucket = ""
				}
				p.Cursor = 0
				p.Marked = map[string]bool{}
				return true, true
			case "rds-hint":
				return true, false
			}
			return false, false
		}
		if p.Service == "vpc" {
			switch it.Kind {
			case "generic":
				if p.S3Bucket == "" {
					// Enter on a VPC → show category menu.
					p.S3Bucket = it.Value
					p.S3Prefix = ""
					p.Cursor = 0
					p.Marked = map[string]bool{}
					return true, true
				}
			case "vpc-category":
				// Enter on a category → show resources.
				p.S3Prefix = it.Value
				p.Cursor = 0
				p.Marked = map[string]bool{}
				return true, true
			case "back":
				if p.S3Prefix != "" {
					p.S3Prefix = ""
				} else {
					p.S3Bucket = ""
				}
				p.Cursor = 0
				p.Marked = map[string]bool{}
				return true, true
			}
			return false, false
		}
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
		if p.Service == "rds" && p.S3Bucket != "" {
			if p.S3Prefix != "" {
				p.S3Prefix = ""
			} else {
				p.S3Bucket = ""
			}
			p.Cursor = 0
			return
		}
		if (p.Service == "ecs" || p.Service == "ecr") && p.S3Bucket != "" {
			p.S3Bucket = ""
			p.S3Prefix = ""
			p.Cursor = 0
			return
		}
		if p.Service == "vpc" && p.S3Bucket != "" {
			if p.S3Prefix != "" {
				p.S3Prefix = ""
			} else {
				p.S3Bucket = ""
			}
			p.Cursor = 0
			return
		}
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
		target := m.input.Target
		extra := m.input.Extra

		// RDS credentials flow: username → password → connect.
		if target == "rds_creds_user" {
			if m.pendingRdsCreds != nil {
				m.pendingRdsCreds.Username = value
			}
			passInput := newInputState(
				fmt.Sprintf("Connect to RDS: %s", func() string {
					if m.pendingRdsCreds != nil {
						return m.pendingRdsCreds.InstanceID
					}
					return ""
				}()),
				"Password:",
				"rds_creds_pass",
				m.input.PanelID, "", "")
			passInput.Input.EchoMode = textinput.EchoPassword
			m.input = passInput
			m.status = "Enter RDS password (input is masked)"
			return m, cmd
		}
		if target == "rds_creds_pass" {
			if m.pendingRdsCreds != nil {
				pending := m.pendingRdsCreds
				m.pendingRdsCreds = nil
				m.mode = modeNormal
				m.status = "Connecting to RDS..."
				return m, rdsConnectCmd(pending, value)
			}
			m.mode = modeNormal
			return m, nil
		}

		// RDS create instance — 5-step flow.
		if target == "rds_create_id" {
			if m.pendingRdsCreate != nil {
				m.pendingRdsCreate.Identifier = value
			}
			m.input = newInputState("Create RDS Instance", "Engine (mysql/postgres):", "rds_create_engine", m.input.PanelID, "", value)
			m.status = "Enter engine (mysql or postgres)"
			return m, cmd
		}
		if target == "rds_create_engine" {
			if m.pendingRdsCreate != nil {
				m.pendingRdsCreate.Engine = strings.ToLower(value)
				if m.pendingRdsCreate.Engine == "" {
					m.pendingRdsCreate.Engine = "mysql"
				}
			}
			prev := extra // identifier stored in Extra
			m.input = newInputState("Create RDS Instance", "Instance class (e.g. db.t3.micro):", "rds_create_class", m.input.PanelID, prev, value)
			m.status = "Enter DB instance class"
			return m, cmd
		}
		if target == "rds_create_class" {
			if m.pendingRdsCreate != nil {
				cls := value
				if cls == "" {
					cls = "db.t3.micro"
				}
				m.pendingRdsCreate.Class = cls
			}
			m.input = newInputState("Create RDS Instance", "Master username:", "rds_create_user", m.input.PanelID, extra, value)
			m.status = "Enter master username"
			return m, cmd
		}
		if target == "rds_create_user" {
			if m.pendingRdsCreate != nil {
				m.pendingRdsCreate.Username = value
			}
			passInput := newInputState("Create RDS Instance", "Master password:", "rds_create_pass", m.input.PanelID, extra, value)
			passInput.Input.EchoMode = textinput.EchoPassword
			m.input = passInput
			m.status = "Enter master password (input is masked)"
			return m, cmd
		}
		if target == "rds_create_pass" {
			if m.pendingRdsCreate != nil {
				pending := m.pendingRdsCreate
				m.pendingRdsCreate = nil
				m.mode = modeNormal
				payload := strings.Join([]string{pending.Identifier, pending.Engine, pending.Class, pending.Username, value}, "\t")
				p := m.getPanel(pending.PanelID)
				return m, opCmd(p, "create_rds_instance", payload, "")
			}
			m.mode = modeNormal
			return m, nil
		}

		// Secrets Manager create — two-step: name then value.
		if target == "create_secret_name" {
			nextInput := newInputState("Create Secret", "Secret value:", "create_secret_value", m.input.PanelID, "", value)
			nextInput.Input.EchoMode = textinput.EchoPassword
			m.input = nextInput
			m.status = "Enter secret value (input is masked)"
			return m, cmd
		}
		if target == "create_secret_value" {
			secretName := extra
			m.persistContext()
			m.mode = modeNormal
			return m, m.runOp("create_secret", secretName, value)
		}

		// ECS service copy network config — two-step inline, not via runOp.
		if target == "ecs_svc_copy_subnets" {
			if m.pendingEcsSvc != nil {
				m.pendingEcsSvc.DstSubnets = splitTrimComma(value)
			}
			// Show second input for security groups.
			m.input = newInputState(
				"ECS Service Network Config",
				"Destination security group IDs (comma-separated):",
				"ecs_svc_copy_sgs", m.active, "", "")
			m.status = "Enter destination security group IDs"
			return m, cmd
		}
		if target == "ecs_svc_copy_sgs" {
			dstSGs := splitTrimComma(value)
			m.mode = modeNormal
			if m.pendingEcsSvc != nil {
				p := m.pendingEcsSvc
				m.pendingEcsSvc = nil
				conflicts := len(p.Existing)
				if conflicts > 0 {
					m.ecsSvcConf = ecsSvcConflictState{
						Title:             fmt.Sprintf("Found %d existing service(s) in destination cluster. [o] overwrite  [s] skip existing  [esc] cancel", conflicts),
						Src:               p.Src,
						Dst:               p.Dst,
						ServiceArns:       p.ServiceArns,
						Existing:          p.Existing,
						DstSubnets:        p.DstSubnets,
						DstSecurityGroups: dstSGs,
					}
					m.mode = modeEcsSvcConflict
					m.status = fmt.Sprintf("Conflicts detected: %d existing ECS service(s)", conflicts)
					return m, nil
				}
				return m, m.startEcsSvcCopyJob(p.Src, p.Dst, p.ServiceArns, p.SkipExisting, p.Existing, p.DstSubnets, dstSGs)
			}
			return m, nil
		}

		m.persistContext()
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
	if m.mode == modeApigwConflict {
		return renderApigwConflict(m)
	}
	if m.mode == modeEcsConflict {
		return renderEcsConflict(m)
	}
	if m.mode == modeEcsSvcConflict {
		return renderEcsSvcConflict(m)
	}
	if m.mode == modeEcrConflict {
		return renderEcrConflict(m)
	}
	if m.mode == modeSecretsConflict {
		return renderSecretsConflict(m)
	}
	if m.mode == modeRdsCopyConfirm {
		return renderRdsCopyConfirm(m)
	}
	if m.mode == modeGrantRegistry {
		return renderGrantRegistry(m)
	}
	return renderMain(m)
}

func currentTheme(m model) uiTheme {
	if len(uiThemes) == 0 {
		return uiTheme{
			Name:           "Default",
			BaseFg:         "255",
			BaseBg:         "0",
			HeaderFg:       "39",
			ShortcutFg:     "246",
			StatusFg:       "86",
			BorderActive:   "39",
			BorderInactive: "240",
			DialogBorder:   "214",
			LogBorder:      "244",
			ErrorFg:        "203",
		}
	}
	i := m.themeIndex % len(uiThemes)
	if i < 0 {
		i = 0
	}
	return uiThemes[i]
}

func renderMain(m model) string {
	th := currentTheme(m)
	header := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color(th.HeaderFg)).Background(lipgloss.Color(th.BaseBg)).Render("AWS Commander")
	logHeight := 8
	panelWidth := max(40, m.width/2-2)
	panelHeight := max(8, m.height-logHeight-6)
	left := renderPanel("LEFT", m.left, m.active == leftPanel, panelWidth, panelHeight, th)
	right := renderPanel("RIGHT", m.right, m.active == rightPanel, panelWidth, panelHeight, th)
	body := lipgloss.JoinHorizontal(lipgloss.Top, left, right)
	shortcuts := lipgloss.NewStyle().Foreground(lipgloss.Color(th.ShortcutFg)).Background(lipgloss.Color(th.BaseBg)).Render("tab switch | i home | j/k move | enter drill | h/backspace up | space mark item | c copy | m move | a grants registry | u revoke (registry) | t theme | v view | e last error | l full logs | d delete | n create bucket | g refresh | q quit")
	status := lipgloss.NewStyle().Foreground(lipgloss.Color(th.StatusFg)).Background(lipgloss.Color(th.BaseBg)).Render("Status: " + m.status)
	logPane := renderLogPane(m, logHeight)
	root := lipgloss.NewStyle().Background(lipgloss.Color(th.BaseBg)).Foreground(lipgloss.Color(th.BaseFg))
	return root.Render(strings.Join([]string{header, body, shortcuts, status, logPane}, "\n"))
}

func renderPanel(title string, p panelState, active bool, width, height int, th uiTheme) string {
	borderColor := th.BorderInactive
	if active {
		borderColor = th.BorderActive
	}
	box := lipgloss.NewStyle().
		Border(lipgloss.NormalBorder()).
		BorderForeground(lipgloss.Color(borderColor)).
		Foreground(lipgloss.Color(th.BaseFg)).
		Background(lipgloss.Color(th.BaseBg)).
		Padding(0, 1).
		Width(width).
		Height(height)

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
		lines[len(lines)-1] = lipgloss.NewStyle().Foreground(lipgloss.Color(th.ErrorFg)).Render("ERR: " + firstLine(p.Err))
	}

	content := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color(th.HeaderFg)).Background(lipgloss.Color(th.BaseBg)).Render(title) + "\n" + head + "\n\n" + strings.Join(lines, "\n")
	return box.Render(content)
}

func renderInput(m model) string {
	th := currentTheme(m)
	box := lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).BorderForeground(lipgloss.Color(th.DialogBorder)).Foreground(lipgloss.Color(th.BaseFg)).Background(lipgloss.Color(th.BaseBg)).Padding(1, 2)
	return box.Render(m.input.Title + "\n\n" + m.input.Prompt + ":\n" + m.input.Input.View() + "\n\nenter=apply esc=cancel")
}

func renderViewMode(m model) string {
	th := currentTheme(m)
	box := lipgloss.NewStyle().Border(lipgloss.NormalBorder()).BorderForeground(lipgloss.Color(th.DialogBorder)).Foreground(lipgloss.Color(th.BaseFg)).Background(lipgloss.Color(th.BaseBg)).Padding(1, 2).Width(max(80, m.width-4)).Height(max(20, m.height-4))
	return box.Render("Resource Detail\n\n" + m.viewText + "\n\nesc/q/enter to close")
}

func renderConfirm(m model) string {
	th := currentTheme(m)
	box := lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).BorderForeground(lipgloss.Color(th.DialogBorder)).Foreground(lipgloss.Color(th.BaseFg)).Background(lipgloss.Color(th.BaseBg)).Padding(1, 2)
	return box.Render(m.confirm.Title + "\n")
}

func renderConflict(m model) string {
	th := currentTheme(m)
	box := lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).BorderForeground(lipgloss.Color(th.DialogBorder)).Foreground(lipgloss.Color(th.BaseFg)).Background(lipgloss.Color(th.BaseBg)).Padding(1, 2).Width(max(70, m.width-4))
	return box.Render("Copy Conflict\n\n" + m.conflict.Title + "\n\n[o] overwrite all\n[s] skip existing\n[esc] cancel")
}

func renderLambdaConflict(m model) string {
	th := currentTheme(m)
	box := lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).BorderForeground(lipgloss.Color(th.DialogBorder)).Foreground(lipgloss.Color(th.BaseFg)).Background(lipgloss.Color(th.BaseBg)).Padding(1, 2).Width(max(70, m.width-4))
	return box.Render("Lambda Copy Conflict\n\n" + m.lconf.Title + "\n\n[o] overwrite all\n[s] skip existing\n[esc] cancel")
}

func renderApigwConflict(m model) string {
	th := currentTheme(m)
	box := lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).BorderForeground(lipgloss.Color(th.DialogBorder)).Foreground(lipgloss.Color(th.BaseFg)).Background(lipgloss.Color(th.BaseBg)).Padding(1, 2).Width(max(70, m.width-4))
	return box.Render("API Gateway Copy Conflict\n\n" + m.apigwConf.Title + "\n\n[o] overwrite all\n[s] skip existing\n[esc] cancel")
}

func renderEcsConflict(m model) string {
	th := currentTheme(m)
	box := lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).BorderForeground(lipgloss.Color(th.DialogBorder)).Foreground(lipgloss.Color(th.BaseFg)).Background(lipgloss.Color(th.BaseBg)).Padding(1, 2).Width(max(70, m.width-4))
	return box.Render("ECS Copy Conflict\n\n" + m.ecsConf.Title + "\n\n[o] overwrite existing\n[s] skip existing\n[esc] cancel")
}

func renderEcsSvcConflict(m model) string {
	th := currentTheme(m)
	box := lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).BorderForeground(lipgloss.Color(th.DialogBorder)).Foreground(lipgloss.Color(th.BaseFg)).Background(lipgloss.Color(th.BaseBg)).Padding(1, 2).Width(max(70, m.width-4))
	return box.Render("ECS Service Copy Conflict\n\n" + m.ecsSvcConf.Title + "\n\n[o] overwrite existing\n[s] skip existing\n[esc] cancel")
}

func renderEcrConflict(m model) string {
	th := currentTheme(m)
	box := lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).BorderForeground(lipgloss.Color(th.DialogBorder)).Foreground(lipgloss.Color(th.BaseFg)).Background(lipgloss.Color(th.BaseBg)).Padding(1, 2).Width(max(70, m.width-4))
	return box.Render("ECR Copy Conflict\n\n" + m.ecrConf.Title + "\n\n[o] overwrite images\n[s] skip existing repos\n[esc] cancel")
}

func renderSecretsConflict(m model) string {
	th := currentTheme(m)
	box := lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).BorderForeground(lipgloss.Color(th.DialogBorder)).Foreground(lipgloss.Color(th.BaseFg)).Background(lipgloss.Color(th.BaseBg)).Padding(1, 2).Width(max(70, m.width-4))
	return box.Render("Secrets Manager Copy Conflict\n\n" + m.secretsConf.Title + "\n\n[o] overwrite all\n[s] skip existing\n[esc] cancel")
}

func renderGrantRegistry(m model) string {
	th := currentTheme(m)
	box := lipgloss.NewStyle().Border(lipgloss.NormalBorder()).BorderForeground(lipgloss.Color(th.DialogBorder)).Foreground(lipgloss.Color(th.BaseFg)).Background(lipgloss.Color(th.BaseBg)).Padding(0, 1).Width(max(80, m.width-2)).Height(max(14, m.height-2))
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
	th := currentTheme(m)
	width := max(40, m.width-2)
	box := lipgloss.NewStyle().
		Border(lipgloss.NormalBorder()).
		BorderForeground(lipgloss.Color(th.LogBorder)).
		Foreground(lipgloss.Color(th.BaseFg)).
		Background(lipgloss.Color(th.BaseBg)).
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
		if p.Service == "ecs" && p.S3Bucket != "" {
			clusterName := p.S3Bucket
			if idx := strings.LastIndex(p.S3Bucket, "/"); idx >= 0 && idx+1 < len(p.S3Bucket) {
				clusterName = p.S3Bucket[idx+1:]
			}
			return fmt.Sprintf("%s > %s > ecs > %s > Services", p.Profile, p.Region, clusterName)
		}
		if p.Service == "ecr" && p.S3Bucket != "" {
			return fmt.Sprintf("%s > %s > ecr > %s > Images", p.Profile, p.Region, p.S3Bucket)
		}
		if p.Service == "vpc" && p.S3Bucket != "" {
			if p.S3Prefix != "" {
				return fmt.Sprintf("%s > %s > vpc > %s > %s", p.Profile, p.Region, p.S3Bucket, p.S3Prefix)
			}
			return fmt.Sprintf("%s > %s > vpc > %s", p.Profile, p.Region, p.S3Bucket)
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
	case "cloudformation":
		cli := cloudformation.NewFromConfig(cfg)
		statusFilter := []cloudformationtypes.StackStatus{
			cloudformationtypes.StackStatusCreateInProgress,
			cloudformationtypes.StackStatusCreateFailed,
			cloudformationtypes.StackStatusCreateComplete,
			cloudformationtypes.StackStatusRollbackInProgress,
			cloudformationtypes.StackStatusRollbackFailed,
			cloudformationtypes.StackStatusRollbackComplete,
			cloudformationtypes.StackStatusDeleteInProgress,
			cloudformationtypes.StackStatusDeleteFailed,
			cloudformationtypes.StackStatusUpdateInProgress,
			cloudformationtypes.StackStatusUpdateCompleteCleanupInProgress,
			cloudformationtypes.StackStatusUpdateComplete,
			cloudformationtypes.StackStatusUpdateFailed,
			cloudformationtypes.StackStatusUpdateRollbackInProgress,
			cloudformationtypes.StackStatusUpdateRollbackFailed,
			cloudformationtypes.StackStatusUpdateRollbackCompleteCleanupInProgress,
			cloudformationtypes.StackStatusUpdateRollbackComplete,
			cloudformationtypes.StackStatusReviewInProgress,
			cloudformationtypes.StackStatusImportInProgress,
			cloudformationtypes.StackStatusImportComplete,
			cloudformationtypes.StackStatusImportRollbackInProgress,
			cloudformationtypes.StackStatusImportRollbackFailed,
			cloudformationtypes.StackStatusImportRollbackComplete,
		}
		out, err := cli.ListStacks(ctx, &cloudformation.ListStacksInput{
			StackStatusFilter: statusFilter,
		})
		if err != nil {
			return nil, err
		}
		items := make([]resourceItem, 0, len(out.StackSummaries))
		for _, s := range out.StackSummaries {
			if s.StackName != nil && *s.StackName != "" {
				items = append(items, resourceItem{Label: *s.StackName, Value: *s.StackName, Kind: "generic"})
			}
		}
		sort.Slice(items, func(i, j int) bool { return items[i].Label < items[j].Label })
		return items, nil
	case "ecs":
		cli := ecs.NewFromConfig(cfg)
		if p.S3Bucket != "" {
			// Drill-down: list services inside the cluster.
			items := []resourceItem{{Label: "..", Value: p.S3Bucket, Kind: "back"}}
			var token *string
			for {
				out, err := cli.ListServices(ctx, &ecs.ListServicesInput{
					Cluster:    aws.String(p.S3Bucket),
					NextToken:  token,
				})
				if err != nil {
					return nil, err
				}
				if len(out.ServiceArns) > 0 {
					desc, derr := cli.DescribeServices(ctx, &ecs.DescribeServicesInput{
						Cluster:  aws.String(p.S3Bucket),
						Services: out.ServiceArns,
					})
					if derr != nil {
						return nil, derr
					}
					for _, svc := range desc.Services {
						if svc.ServiceName == nil {
							continue
						}
						launchType := string(svc.LaunchType)
						if launchType == "" {
							launchType = "EC2"
						}
						taskDef := aws.ToString(svc.TaskDefinition)
						if idx := strings.LastIndex(taskDef, "/"); idx >= 0 {
							taskDef = taskDef[idx+1:]
						}
						label := fmt.Sprintf("%-40s  %-8s  d:%d r:%d p:%d  %-8s  %s",
							*svc.ServiceName,
							aws.ToString(svc.Status),
							svc.DesiredCount, svc.RunningCount, svc.PendingCount,
							launchType, taskDef)
						items = append(items, resourceItem{Label: label, Value: aws.ToString(svc.ServiceArn), Kind: "generic"})
					}
				}
				if out.NextToken == nil || strings.TrimSpace(*out.NextToken) == "" {
					break
				}
				token = out.NextToken
			}
			sort.Slice(items[1:], func(i, j int) bool { return items[i+1].Label < items[j+1].Label })
			return items, nil
		}
		// Top-level: list clusters.
		items := make([]resourceItem, 0, 64)
		var token *string
		for {
			out, err := cli.ListClusters(ctx, &ecs.ListClustersInput{NextToken: token})
			if err != nil {
				return nil, err
			}
			for _, arn := range out.ClusterArns {
				name := arn
				if idx := strings.LastIndex(arn, "/"); idx >= 0 && idx+1 < len(arn) {
					name = arn[idx+1:]
				}
				items = append(items, resourceItem{Label: name, Value: arn, Kind: "generic"})
			}
			if out.NextToken == nil || strings.TrimSpace(*out.NextToken) == "" {
				break
			}
			token = out.NextToken
		}
		sort.Slice(items, func(i, j int) bool { return items[i].Label < items[j].Label })
		return items, nil
	case "ecr":
		cli := ecr.NewFromConfig(cfg)
		if p.S3Bucket != "" {
			// Drill-down: list images inside a repository.
			items := []resourceItem{{Label: "..", Value: p.S3Bucket, Kind: "back"}}
			var imgToken *string
			for {
				out, err := cli.DescribeImages(ctx, &ecr.DescribeImagesInput{
					RepositoryName: aws.String(p.S3Bucket),
					NextToken:      imgToken,
				})
				if err != nil {
					return nil, err
				}
				for _, img := range out.ImageDetails {
					digest := aws.ToString(img.ImageDigest)
					shortDigest := digest
					if len(digest) > 19 {
						shortDigest = digest[:19]
					}
					size := ""
					if img.ImageSizeInBytes != nil {
						size = formatECRImageSize(*img.ImageSizeInBytes)
					}
					pushed := ""
					if img.ImagePushedAt != nil {
						pushed = img.ImagePushedAt.Format("2006-01-02")
					}
					tags := strings.Join(img.ImageTags, ", ")
					if tags == "" {
						tags = "<untagged>"
					}
					label := fmt.Sprintf("%-35s  %s  %8s  %s", tags, shortDigest, size, pushed)
					items = append(items, resourceItem{Label: label, Value: digest, Kind: "ecr-image"})
				}
				if out.NextToken == nil || strings.TrimSpace(*out.NextToken) == "" {
					break
				}
				imgToken = out.NextToken
			}
			sort.Slice(items[1:], func(i, j int) bool { return items[i+1].Label < items[j+1].Label })
			return items, nil
		}
		// Top-level: list repositories.
		items := make([]resourceItem, 0, 128)
		var token *string
		for {
			out, err := cli.DescribeRepositories(ctx, &ecr.DescribeRepositoriesInput{NextToken: token})
			if err != nil {
				return nil, err
			}
			for _, r := range out.Repositories {
				if r.RepositoryName == nil {
					continue
				}
				items = append(items, resourceItem{Label: *r.RepositoryName, Value: *r.RepositoryName, Kind: "generic"})
			}
			if out.NextToken == nil || strings.TrimSpace(*out.NextToken) == "" {
				break
			}
			token = out.NextToken
		}
		sort.Slice(items, func(i, j int) bool { return items[i].Label < items[j].Label })
		return items, nil
	case "vpc":
		cli := ec2.NewFromConfig(cfg)
		if p.S3Bucket != "" && p.S3Prefix != "" {
			// Category level: list resources of the chosen category inside the VPC.
			vpcID := p.S3Bucket
			filter := []ec2types.Filter{{Name: aws.String("vpc-id"), Values: []string{vpcID}}}
			items := []resourceItem{{Label: "..", Value: p.S3Bucket, Kind: "back"}}
			switch p.S3Prefix {
			case "subnets":
				out2, err := cli.DescribeSubnets(ctx, &ec2.DescribeSubnetsInput{Filters: filter})
				if err != nil {
					return nil, err
				}
				for _, s := range out2.Subnets {
					name := ec2TagName(s.Tags)
					cidr := aws.ToString(s.CidrBlock)
					az := aws.ToString(s.AvailabilityZone)
					pub := "private"
					if aws.ToBool(s.MapPublicIpOnLaunch) {
						pub = "public"
					}
					avail := fmt.Sprintf("%d avail", aws.ToInt32(s.AvailableIpAddressCount))
					label := fmt.Sprintf("%-24s  %-18s  %-15s  %-8s  %s", aws.ToString(s.SubnetId), cidr, az, pub, name)
					_ = avail
					items = append(items, resourceItem{Label: label, Value: aws.ToString(s.SubnetId), Kind: "generic"})
				}
			case "security-groups":
				out2, err := cli.DescribeSecurityGroups(ctx, &ec2.DescribeSecurityGroupsInput{Filters: filter})
				if err != nil {
					return nil, err
				}
				for _, sg := range out2.SecurityGroups {
					label := fmt.Sprintf("%-24s  %-20s  %s", aws.ToString(sg.GroupId), aws.ToString(sg.GroupName), aws.ToString(sg.Description))
					items = append(items, resourceItem{Label: label, Value: aws.ToString(sg.GroupId), Kind: "generic"})
				}
			case "route-tables":
				out2, err := cli.DescribeRouteTables(ctx, &ec2.DescribeRouteTablesInput{Filters: filter})
				if err != nil {
					return nil, err
				}
				for _, rt := range out2.RouteTables {
					name := ec2TagName(rt.Tags)
					main := ""
					for _, assoc := range rt.Associations {
						if aws.ToBool(assoc.Main) {
							main = " [main]"
							break
						}
					}
					label := fmt.Sprintf("%-24s  %d routes%s  %s", aws.ToString(rt.RouteTableId), len(rt.Routes), main, name)
					items = append(items, resourceItem{Label: label, Value: aws.ToString(rt.RouteTableId), Kind: "generic"})
				}
			case "instances":
				out2, err := cli.DescribeInstances(ctx, &ec2.DescribeInstancesInput{Filters: filter})
				if err != nil {
					return nil, err
				}
				for _, res := range out2.Reservations {
					for _, inst := range res.Instances {
						name := ec2TagName(inst.Tags)
						state := string(inst.State.Name)
						itype := string(inst.InstanceType)
						ip := aws.ToString(inst.PrivateIpAddress)
						label := fmt.Sprintf("%-22s  %-14s  %-10s  %-16s  %s", aws.ToString(inst.InstanceId), itype, state, ip, name)
						items = append(items, resourceItem{Label: label, Value: aws.ToString(inst.InstanceId), Kind: "generic"})
					}
				}
			case "gateways":
				// Internet Gateways
				igwOut, err := cli.DescribeInternetGateways(ctx, &ec2.DescribeInternetGatewaysInput{
					Filters: []ec2types.Filter{{Name: aws.String("attachment.vpc-id"), Values: []string{vpcID}}},
				})
				if err == nil {
					for _, igw := range igwOut.InternetGateways {
						name := ec2TagName(igw.Tags)
						label := fmt.Sprintf("%-24s  IGW      attached  %s", aws.ToString(igw.InternetGatewayId), name)
						items = append(items, resourceItem{Label: label, Value: aws.ToString(igw.InternetGatewayId), Kind: "generic"})
					}
				}
				// NAT Gateways
				natOut, err := cli.DescribeNatGateways(ctx, &ec2.DescribeNatGatewaysInput{
					Filter: []ec2types.Filter{{Name: aws.String("vpc-id"), Values: []string{vpcID}}},
				})
				if err == nil {
					for _, nat := range natOut.NatGateways {
						if nat.State == "deleted" {
							continue
						}
						name := ec2TagName(nat.Tags)
						ip := ""
						if len(nat.NatGatewayAddresses) > 0 {
							ip = aws.ToString(nat.NatGatewayAddresses[0].PublicIp)
						}
						label := fmt.Sprintf("%-24s  NAT  %-12s  %-16s  %s", aws.ToString(nat.NatGatewayId), string(nat.State), ip, name)
						items = append(items, resourceItem{Label: label, Value: aws.ToString(nat.NatGatewayId), Kind: "generic"})
					}
				}
			}
			return items, nil
		}
		if p.S3Bucket != "" {
			// VPC level: show category menu.
			return []resourceItem{
				{Label: "..", Value: p.S3Bucket, Kind: "back"},
				{Label: "Subnets", Value: "subnets", Kind: "vpc-category"},
				{Label: "Security Groups", Value: "security-groups", Kind: "vpc-category"},
				{Label: "Route Tables", Value: "route-tables", Kind: "vpc-category"},
				{Label: "Instances", Value: "instances", Kind: "vpc-category"},
				{Label: "Gateways (IGW + NAT)", Value: "gateways", Kind: "vpc-category"},
			}, nil
		}
		// Top-level: list VPCs with Name tag and CIDR.
		out, err := cli.DescribeVpcs(ctx, &ec2.DescribeVpcsInput{})
		if err != nil {
			return nil, err
		}
		items := make([]resourceItem, 0, len(out.Vpcs))
		for _, v := range out.Vpcs {
			if v.VpcId == nil {
				continue
			}
			name := ec2TagName(v.Tags)
			cidr := aws.ToString(v.CidrBlock)
			dflt := ""
			if aws.ToBool(v.IsDefault) {
				dflt = " [default]"
			}
			label := fmt.Sprintf("%-24s  %-18s  %s%s", *v.VpcId, cidr, name, dflt)
			items = append(items, resourceItem{Label: label, Value: *v.VpcId, Kind: "generic"})
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
			if api.ApiId == nil || api.Name == nil {
				continue
			}
			proto := "HTTP"
			if api.ProtocolType == apigwtypes.ProtocolTypeWebsocket {
				proto = "WS"
			}
			endpoint := ""
			if api.ApiEndpoint != nil {
				// Strip https:// prefix to save space.
				endpoint = strings.TrimPrefix(*api.ApiEndpoint, "https://")
			}
			label := fmt.Sprintf("%s  [%s]  %s", *api.Name, proto, endpoint)
			items = append(items, resourceItem{Label: label, Value: *api.ApiId, Kind: "generic"})
		}
		sort.Slice(items, func(i, j int) bool { return items[i].Label < items[j].Label })
		return items, nil
	case "secretsmanager":
		cli := secretsmanager.NewFromConfig(cfg)
		items := make([]resourceItem, 0)
		var smToken *string
		for {
			out, err := cli.ListSecrets(ctx, &secretsmanager.ListSecretsInput{NextToken: smToken})
			if err != nil {
				return nil, err
			}
			for _, s := range out.SecretList {
				name := aws.ToString(s.Name)
				desc := aws.ToString(s.Description)
				label := name
				if desc != "" {
					label = fmt.Sprintf("%-45s  %s", name, desc)
				}
				items = append(items, resourceItem{Label: label, Value: name, Kind: "generic"})
			}
			if out.NextToken == nil || aws.ToString(out.NextToken) == "" {
				break
			}
			smToken = out.NextToken
		}
		sort.Slice(items, func(i, j int) bool { return items[i].Label < items[j].Label })
		return items, nil
	case "rds":
		// Level 1: list instances and clusters.
		if p.S3Bucket == "" {
			rdsCli := awsrds.NewFromConfig(cfg)
			items := make([]resourceItem, 0)
			var instToken *string
			for {
				out, err := rdsCli.DescribeDBInstances(ctx, &awsrds.DescribeDBInstancesInput{Marker: instToken})
				if err != nil {
					return nil, err
				}
				for _, inst := range out.DBInstances {
					id := aws.ToString(inst.DBInstanceIdentifier)
					engine := aws.ToString(inst.Engine)
					status := aws.ToString(inst.DBInstanceStatus)
					class := aws.ToString(inst.DBInstanceClass)
					label := fmt.Sprintf("%-40s  %-20s  %-12s  %s", id, engine, status, class)
					items = append(items, resourceItem{Label: label, Value: id, Kind: "rds-instance"})
				}
				if out.Marker == nil || aws.ToString(out.Marker) == "" {
					break
				}
				instToken = out.Marker
			}
			var clusterToken *string
			for {
				out, err := rdsCli.DescribeDBClusters(ctx, &awsrds.DescribeDBClustersInput{Marker: clusterToken})
				if err != nil {
					break // clusters may not be supported in all regions; ignore error
				}
				for _, cl := range out.DBClusters {
					id := aws.ToString(cl.DBClusterIdentifier)
					engine := aws.ToString(cl.Engine)
					status := aws.ToString(cl.Status)
					label := fmt.Sprintf("%-40s  %-20s  %-12s  [cluster]", id, engine, status)
					items = append(items, resourceItem{Label: label, Value: id, Kind: "rds-cluster"})
				}
				if out.Marker == nil || aws.ToString(out.Marker) == "" {
					break
				}
				clusterToken = out.Marker
			}
			sort.Slice(items, func(i, j int) bool { return items[i].Label < items[j].Label })
			return items, nil
		}
		// Level 2: list databases in instance (via SQL).
		if p.S3Bucket != "" && p.S3Prefix == "" {
			cacheKey := rdsCacheKey(p.Profile, p.Region, p.S3Bucket)
			rdsConnCache.RLock()
			conn, hasConn := rdsConnCache.conns[cacheKey]
			creds, hasCreds := rdsConnCache.creds[cacheKey]
			rdsConnCache.RUnlock()
			if !hasConn || !hasCreds {
				return []resourceItem{{Label: "← No connection. Press Backspace to go back, then Enter on the instance to connect.", Kind: "rds-hint", Value: ""}}, nil
			}
			dbs, err := rdsListDatabases(ctx, conn, creds.Engine)
			if err != nil {
				return nil, fmt.Errorf("list databases: %w", err)
			}
			items := []resourceItem{{Label: "← ..", Kind: "back", Value: ""}}
			items = append(items, dbs...)
			return items, nil
		}
		// Level 3: list tables in database (via SQL).
		if p.S3Bucket != "" && p.S3Prefix != "" {
			cacheKey := rdsCacheKey(p.Profile, p.Region, p.S3Bucket)
			rdsConnCache.RLock()
			creds, hasCreds := rdsConnCache.creds[cacheKey]
			conn, hasConn := rdsConnCache.conns[cacheKey]
			if hasCreds && isPostgres(creds.Engine) {
				pgKey := cacheKey + ":" + p.S3Prefix
				if pgConn, ok := rdsConnCache.conns[pgKey]; ok {
					conn = pgConn
					hasConn = true
				}
			}
			rdsConnCache.RUnlock()
			if !hasConn || !hasCreds {
				return []resourceItem{{Label: "← No connection. Press Backspace to reconnect.", Kind: "rds-hint", Value: ""}}, nil
			}
			tables, err := rdsListTables(ctx, conn, creds.Engine, p.S3Prefix)
			if err != nil {
				return nil, fmt.Errorf("list tables: %w", err)
			}
			items := []resourceItem{{Label: "← ..", Kind: "back", Value: ""}}
			items = append(items, tables...)
			return items, nil
		}
		return nil, nil
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
		if p.S3Bucket != "" && p.S3Prefix != "" {
			// Detail for a resource inside a VPC category.
			switch p.S3Prefix {
			case "subnets":
				out2, err := cli.DescribeSubnets(ctx, &ec2.DescribeSubnetsInput{SubnetIds: []string{it.Value}})
				if err != nil {
					return "", err
				}
				payload = out2
			case "security-groups":
				out2, err := cli.DescribeSecurityGroups(ctx, &ec2.DescribeSecurityGroupsInput{GroupIds: []string{it.Value}})
				if err != nil {
					return "", err
				}
				payload = out2
			case "route-tables":
				out2, err := cli.DescribeRouteTables(ctx, &ec2.DescribeRouteTablesInput{RouteTableIds: []string{it.Value}})
				if err != nil {
					return "", err
				}
				payload = out2
			case "instances":
				out2, err := cli.DescribeInstances(ctx, &ec2.DescribeInstancesInput{InstanceIds: []string{it.Value}})
				if err != nil {
					return "", err
				}
				payload = out2
			case "gateways":
				if strings.HasPrefix(it.Value, "igw-") {
					out2, err := cli.DescribeInternetGateways(ctx, &ec2.DescribeInternetGatewaysInput{InternetGatewayIds: []string{it.Value}})
					if err != nil {
						return "", err
					}
					payload = out2
				} else {
					out2, err := cli.DescribeNatGateways(ctx, &ec2.DescribeNatGatewaysInput{NatGatewayIds: []string{it.Value}})
					if err != nil {
						return "", err
					}
					payload = out2
				}
			default:
				payload = map[string]any{"item": it}
			}
		} else {
			out, err := cli.DescribeVpcs(ctx, &ec2.DescribeVpcsInput{VpcIds: []string{it.Value}})
			if err != nil {
				return "", err
			}
			payload = out
		}
	case "apigateway":
		cli := apigatewayv2.NewFromConfig(cfg)
		out, err := cli.GetApi(ctx, &apigatewayv2.GetApiInput{ApiId: aws.String(it.Value)})
		if err != nil {
			return "", err
		}
		payload = out
	case "cloudformation":
		cli := cloudformation.NewFromConfig(cfg)
		out, err := cli.DescribeStacks(ctx, &cloudformation.DescribeStacksInput{StackName: aws.String(it.Value)})
		if err != nil {
			return "", err
		}
		payload = out
	case "ecs":
		cli := ecs.NewFromConfig(cfg)
		if p.S3Bucket != "" {
			// Viewing a service inside a cluster.
			out, err := cli.DescribeServices(ctx, &ecs.DescribeServicesInput{
				Cluster:  aws.String(p.S3Bucket),
				Services: []string{it.Value},
			})
			if err != nil {
				return "", err
			}
			payload = out
		} else {
			out, err := cli.DescribeClusters(ctx, &ecs.DescribeClustersInput{Clusters: []string{it.Value}})
			if err != nil {
				return "", err
			}
			payload = out
		}
	case "ecr":
		cli := ecr.NewFromConfig(cfg)
		if p.S3Bucket != "" && it.Kind == "ecr-image" {
			// Detail for an image inside a repository.
			out, err := cli.DescribeImages(ctx, &ecr.DescribeImagesInput{
				RepositoryName: aws.String(p.S3Bucket),
				ImageIds:       []ecrtypes.ImageIdentifier{{ImageDigest: aws.String(it.Value)}},
			})
			if err != nil {
				return "", err
			}
			payload = out
		} else {
			out, err := cli.DescribeRepositories(ctx, &ecr.DescribeRepositoriesInput{RepositoryNames: []string{it.Value}})
			if err != nil {
				return "", err
			}
			payload = out
		}
	case "secretsmanager":
		cli := secretsmanager.NewFromConfig(cfg)
		desc, err := cli.DescribeSecret(ctx, &secretsmanager.DescribeSecretInput{SecretId: aws.String(it.Value)})
		if err != nil {
			return "", err
		}
		valOut, valErr := cli.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: aws.String(it.Value)})
		result := map[string]any{"description": desc}
		if valErr == nil {
			if valOut.SecretString != nil {
				result["secret_string"] = *valOut.SecretString
			} else {
				result["secret_binary"] = "(binary value)"
			}
		} else {
			result["secret_value_error"] = valErr.Error()
		}
		payload = result
	case "rds":
		rdsCli := awsrds.NewFromConfig(cfg)
		if it.Kind == "rds-table" {
			// SQL SELECT rows from table.
			cacheKey := rdsCacheKey(p.Profile, p.Region, p.S3Bucket)
			rdsConnCache.RLock()
			creds, hasCreds := rdsConnCache.creds[cacheKey]
			conn, hasConn := rdsConnCache.conns[cacheKey]
			if hasCreds && isPostgres(creds.Engine) {
				pgKey := cacheKey + ":" + p.S3Prefix
				if pgConn, ok := rdsConnCache.conns[pgKey]; ok {
					conn = pgConn
					hasConn = true
				}
			}
			rdsConnCache.RUnlock()
			if !hasConn || !hasCreds {
				return "No active connection. Go back and reconnect to the instance.", nil
			}
			rows, err := rdsSelectRows(ctx, conn, creds.Engine, p.S3Prefix, it.Value, 50)
			if err != nil {
				return "", err
			}
			return rows, nil
		}
		if it.Kind == "rds-cluster" {
			out, err := rdsCli.DescribeDBClusters(ctx, &awsrds.DescribeDBClustersInput{
				DBClusterIdentifier: aws.String(it.Value),
			})
			if err != nil {
				return "", err
			}
			if len(out.DBClusters) > 0 {
				payload = out.DBClusters[0]
			} else {
				payload = map[string]any{"id": it.Value}
			}
		} else {
			out, err := rdsCli.DescribeDBInstances(ctx, &awsrds.DescribeDBInstancesInput{
				DBInstanceIdentifier: aws.String(it.Value),
			})
			if err != nil {
				return "", err
			}
			if len(out.DBInstances) > 0 {
				payload = out.DBInstances[0]
			} else {
				payload = map[string]any{"id": it.Value}
			}
		}
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

func deleteCloudFormationStack(ctx context.Context, profile, region, stackName string) error {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(resolveProfileCase(profile)),
		config.WithRegion(region),
	)
	if err != nil {
		return err
	}
	cli := cloudformation.NewFromConfig(cfg)
	_, err = cli.DeleteStack(ctx, &cloudformation.DeleteStackInput{StackName: aws.String(stackName)})
	return err
}

func deleteECSCluster(ctx context.Context, profile, region, clusterArn string) error {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(resolveProfileCase(profile)),
		config.WithRegion(region),
	)
	if err != nil {
		return err
	}
	cli := ecs.NewFromConfig(cfg)

	// Stop all running tasks first.
	var taskToken *string
	for {
		tasksOut, terr := cli.ListTasks(ctx, &ecs.ListTasksInput{Cluster: aws.String(clusterArn), NextToken: taskToken})
		if terr != nil {
			break
		}
		for _, taskArn := range tasksOut.TaskArns {
			cli.StopTask(ctx, &ecs.StopTaskInput{ //nolint:errcheck
				Cluster: aws.String(clusterArn),
				Task:    aws.String(taskArn),
				Reason:  aws.String("Cluster deletion"),
			})
		}
		if tasksOut.NextToken == nil || *tasksOut.NextToken == "" {
			break
		}
		taskToken = tasksOut.NextToken
	}

	// Delete all services (scale to 0 + force delete).
	var svcToken *string
	for {
		svcsOut, serr := cli.ListServices(ctx, &ecs.ListServicesInput{Cluster: aws.String(clusterArn), NextToken: svcToken})
		if serr != nil {
			break
		}
		for _, svcArn := range svcsOut.ServiceArns {
			cli.UpdateService(ctx, &ecs.UpdateServiceInput{ //nolint:errcheck
				Cluster:      aws.String(clusterArn),
				Service:      aws.String(svcArn),
				DesiredCount: aws.Int32(0),
			})
			cli.DeleteService(ctx, &ecs.DeleteServiceInput{ //nolint:errcheck
				Cluster: aws.String(clusterArn),
				Service: aws.String(svcArn),
				Force:   aws.Bool(true),
			})
		}
		if svcsOut.NextToken == nil || *svcsOut.NextToken == "" {
			break
		}
		svcToken = svcsOut.NextToken
	}

	_, err = cli.DeleteCluster(ctx, &ecs.DeleteClusterInput{Cluster: aws.String(clusterArn)})
	return err
}

func deleteECSService(ctx context.Context, profile, region, clusterArn, serviceArn string) error {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(resolveProfileCase(profile)),
		config.WithRegion(region),
	)
	if err != nil {
		return err
	}
	cli := ecs.NewFromConfig(cfg)
	// Scale to 0 before force-deleting so tasks stop.
	cli.UpdateService(ctx, &ecs.UpdateServiceInput{ //nolint:errcheck
		Cluster:      aws.String(clusterArn),
		Service:      aws.String(serviceArn),
		DesiredCount: aws.Int32(0),
	})
	_, err = cli.DeleteService(ctx, &ecs.DeleteServiceInput{
		Cluster: aws.String(clusterArn),
		Service: aws.String(serviceArn),
		Force:   aws.Bool(true),
	})
	return err
}

func deleteECRRepository(ctx context.Context, profile, region, repoName string) error {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(resolveProfileCase(profile)),
		config.WithRegion(region),
	)
	if err != nil {
		return err
	}
	cli := ecr.NewFromConfig(cfg)
	_, err = cli.DeleteRepository(ctx, &ecr.DeleteRepositoryInput{
		RepositoryName: aws.String(repoName),
		Force:          true,
	})
	return err
}

func deleteECRImage(ctx context.Context, profile, region, repoName, imageDigest string) error {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(resolveProfileCase(profile)),
		config.WithRegion(region),
	)
	if err != nil {
		return err
	}
	cli := ecr.NewFromConfig(cfg)
	out, err := cli.BatchDeleteImage(ctx, &ecr.BatchDeleteImageInput{
		RepositoryName: aws.String(repoName),
		ImageIds:       []ecrtypes.ImageIdentifier{{ImageDigest: aws.String(imageDigest)}},
	})
	if err != nil {
		return err
	}
	if len(out.Failures) > 0 {
		f := out.Failures[0]
		return fmt.Errorf("delete image failure: %s", aws.ToString(f.FailureReason))
	}
	return nil
}

func createECRRepository(ctx context.Context, profile, region, repoName string) error {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(resolveProfileCase(profile)),
		config.WithRegion(region),
	)
	if err != nil {
		return err
	}
	cli := ecr.NewFromConfig(cfg)
	_, err = cli.CreateRepository(ctx, &ecr.CreateRepositoryInput{
		RepositoryName: aws.String(repoName),
	})
	return err
}

// ec2TagName returns the value of the "Name" tag from an EC2 tags slice.
func ec2TagName(tags []ec2types.Tag) string {
	for _, t := range tags {
		if aws.ToString(t.Key) == "Name" {
			return aws.ToString(t.Value)
		}
	}
	return ""
}

// formatECRImageSize formats an image size in bytes to a human-readable string.
func formatECRImageSize(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%dB", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func deleteAPIGatewayAPI(ctx context.Context, profile, region, apiID string) error {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(resolveProfileCase(profile)),
		config.WithRegion(region),
	)
	if err != nil {
		return err
	}
	cli := apigatewayv2.NewFromConfig(cfg)
	_, err = cli.DeleteApi(ctx, &apigatewayv2.DeleteApiInput{ApiId: aws.String(apiID)})
	return err
}

func prepareCFDeletePreviewCmd(pid panelID, panel panelState, stackName string) tea.Cmd {
	return func() tea.Msg {
		action := fmt.Sprintf("preview cloudformation delete %s profile=%s region=%s", stackName, panel.Profile, panel.Region)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		cfg, err := config.LoadDefaultConfig(ctx,
			config.WithSharedConfigProfile(resolveProfileCase(panel.Profile)),
			config.WithRegion(panel.Region),
		)
		if err != nil {
			return cfDeletePreviewMsg{PanelID: pid, Panel: panel, Stack: stackName, Err: err, Action: action}
		}
		cli := cloudformation.NewFromConfig(cfg)
		var rows []string
		var token *string
		for {
			out, err := cli.ListStackResources(ctx, &cloudformation.ListStackResourcesInput{
				StackName: aws.String(stackName),
				NextToken: token,
			})
			if err != nil {
				return cfDeletePreviewMsg{PanelID: pid, Panel: panel, Stack: stackName, Err: err, Action: action}
			}
			for _, s := range out.StackResourceSummaries {
				logical := aws.ToString(s.LogicalResourceId)
				typ := aws.ToString(s.ResourceType)
				status := string(s.ResourceStatus)
				row := fmt.Sprintf("- %s [%s] (%s)", logical, typ, status)
				rows = append(rows, row)
			}
			if out.NextToken == nil || strings.TrimSpace(*out.NextToken) == "" {
				break
			}
			token = out.NextToken
		}
		sort.Strings(rows)
		const maxLines = 18
		total := len(rows)
		if len(rows) > maxLines {
			rows = rows[:maxLines]
		}
		preview := fmt.Sprintf("Stack: %s\nResources to delete: %d", stackName, total)
		if total == 0 {
			preview += "\n(no resources listed)"
		} else {
			preview += "\n" + strings.Join(rows, "\n")
			if total > maxLines {
				preview += fmt.Sprintf("\n... and %d more", total-maxLines)
			}
		}
		return cfDeletePreviewMsg{
			PanelID: pid,
			Panel:   panel,
			Stack:   stackName,
			Preview: preview,
			Action:  action,
		}
	}
}

func scheduleCFDeleteAutoRefreshCmd(remaining int) tea.Cmd {
	return tea.Tick(2*time.Second, func(time.Time) tea.Msg {
		return cfDeleteAutoRefreshMsg{Remaining: remaining}
	})
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
		case "delete_cf_stack":
			action := fmt.Sprintf("delete cloudformation stack %s profile=%s region=%s", payload, panel.Profile, panel.Region)
			err := deleteCloudFormationStack(ctx, panel.Profile, panel.Region, payload)
			if err != nil {
				return opMsg{Err: err, Action: action}
			}
			return opMsg{Status: "CloudFormation delete started: " + payload, Action: action}
		case "delete_ecs_cluster":
			action := fmt.Sprintf("delete ecs cluster %s profile=%s region=%s", payload, panel.Profile, panel.Region)
			err := deleteECSCluster(ctx, panel.Profile, panel.Region, payload)
			if err != nil {
				return opMsg{Err: err, Action: action}
			}
			return opMsg{Status: "ECS cluster deleted: " + payload, Action: action}
		case "delete_ecs_service":
			action := fmt.Sprintf("delete ecs service %s profile=%s region=%s", payload, panel.Profile, panel.Region)
			err := deleteECSService(ctx, panel.Profile, panel.Region, panel.S3Bucket, payload)
			if err != nil {
				return opMsg{Err: err, Action: action}
			}
			return opMsg{Status: "ECS service deleted: " + payload, Action: action}
		case "delete_ecr_repo":
			action := fmt.Sprintf("delete ecr repository %s profile=%s region=%s", payload, panel.Profile, panel.Region)
			err := deleteECRRepository(ctx, panel.Profile, panel.Region, payload)
			if err != nil {
				return opMsg{Err: err, Action: action}
			}
			return opMsg{Status: "ECR repository deleted: " + payload, Action: action}
		case "delete_ecr_image":
			action := fmt.Sprintf("delete ecr image %s from %s profile=%s region=%s", payload[:min(len(payload), 19)], panel.S3Bucket, panel.Profile, panel.Region)
			err := deleteECRImage(ctx, panel.Profile, panel.Region, panel.S3Bucket, payload)
			if err != nil {
				return opMsg{Err: err, Action: action}
			}
			return opMsg{Status: "ECR image deleted: " + payload[:min(len(payload), 19)], Action: action}
		case "create_ecr_repo":
			action := fmt.Sprintf("create ecr repository %s profile=%s region=%s", payload, panel.Profile, panel.Region)
			err := createECRRepository(ctx, panel.Profile, panel.Region, payload)
			if err != nil {
				return opMsg{Err: err, Action: action}
			}
			return opMsg{Status: "ECR repository created: " + payload, Action: action}
		case "delete_apigateway_api":
			action := fmt.Sprintf("delete apigateway api %s profile=%s region=%s", payload, panel.Profile, panel.Region)
			err := deleteAPIGatewayAPI(ctx, panel.Profile, panel.Region, payload)
			if err != nil {
				return opMsg{Err: err, Action: action}
			}
			return opMsg{Status: "API Gateway deleted: " + payload, Action: action}
		case "create_secret":
		// payload = secret name, extra = secret value
		secretName := payload
		secretValue := extra
		action := fmt.Sprintf("create secret %s profile=%s region=%s", secretName, panel.Profile, panel.Region)
		err := createSecret(ctx, panel.Profile, panel.Region, secretName, secretValue)
		if err != nil {
			return opMsg{Err: err, Action: action}
		}
		return opMsg{Status: "Secret created: " + secretName, Action: action}
	case "delete_secret":
		action := fmt.Sprintf("delete secret %s profile=%s region=%s", payload, panel.Profile, panel.Region)
		err := deleteSecret(ctx, panel.Profile, panel.Region, payload)
		if err != nil {
			return opMsg{Err: err, Action: action}
		}
		return opMsg{Status: "Secret deleted: " + payload, Action: action}
	case "delete_secret_bulk":
		names := make([]string, 0)
		for _, n := range strings.Split(payload, "\n") {
			n = strings.TrimSpace(n)
			if n != "" {
				names = append(names, n)
			}
		}
		action := fmt.Sprintf("delete secrets (%d) profile=%s region=%s", len(names), panel.Profile, panel.Region)
		if len(names) == 0 {
			return opMsg{Status: "No secrets selected", Action: action}
		}
		ctxBulk, cancelBulk := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancelBulk()
		ok := 0
		failed := make([]string, 0)
		for _, name := range names {
			if err := deleteSecret(ctxBulk, panel.Profile, panel.Region, name); err != nil {
				failed = append(failed, fmt.Sprintf("%s (%v)", name, err))
				continue
			}
			ok++
		}
		if len(failed) > 0 {
			return opMsg{
				Err:    fmt.Errorf("partial delete: deleted %d/%d secrets, failed %d. first: %s", ok, len(names), len(failed), failed[0]),
				Action: action,
			}
		}
		return opMsg{Status: fmt.Sprintf("Deleted %d secret(s)", ok), Action: action}
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
	case "create_rds_instance":
		parts := strings.SplitN(payload, "\t", 5)
		if len(parts) < 5 {
			return opMsg{Err: fmt.Errorf("invalid create_rds_instance payload"), Action: "create rds instance"}
		}
		identifier, engine, class, username, password := parts[0], parts[1], parts[2], parts[3], parts[4]
		action := fmt.Sprintf("create rds instance %s profile=%s region=%s", identifier, panel.Profile, panel.Region)
		ctxCreate, cancelCreate := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancelCreate()
		err := createRDSInstance(ctxCreate, panel.Profile, panel.Region, identifier, engine, class, username, password)
		if err != nil {
			return opMsg{Err: err, Action: action}
		}
		return opMsg{Status: "RDS instance creation started: " + identifier, Action: action}
	case "delete_rds_instance":
		action := fmt.Sprintf("delete rds instance %s profile=%s region=%s", payload, panel.Profile, panel.Region)
		err := deleteRDSInstance(ctx, panel.Profile, panel.Region, payload)
		if err != nil {
			return opMsg{Err: err, Action: action}
		}
		return opMsg{Status: "RDS instance deleted: " + payload, Action: action}
	case "delete_rds_bulk":
		ids := make([]string, 0)
		for _, n := range strings.Split(payload, "\n") {
			n = strings.TrimSpace(n)
			if n != "" {
				ids = append(ids, n)
			}
		}
		action := fmt.Sprintf("delete rds instances (%d) profile=%s region=%s", len(ids), panel.Profile, panel.Region)
		if len(ids) == 0 {
			return opMsg{Status: "No RDS instances selected", Action: action}
		}
		ctxBulk, cancelBulk := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancelBulk()
		ok := 0
		failed := make([]string, 0)
		for _, id := range ids {
			if err := deleteRDSInstance(ctxBulk, panel.Profile, panel.Region, id); err != nil {
				failed = append(failed, fmt.Sprintf("%s (%v)", id, err))
				continue
			}
			ok++
		}
		if len(failed) > 0 {
			return opMsg{
				Err:    fmt.Errorf("partial delete: deleted %d/%d, failed %d. first: %s", ok, len(ids), len(failed), failed[0]),
				Action: action,
			}
		}
		return opMsg{Status: fmt.Sprintf("Deleted %d RDS instance(s)", ok), Action: action}
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

// toggleAllSelectableItems inverts the selection of every selectable item:
// marked → unmarked, unmarked → marked. Returns the count of items now marked.
func toggleAllSelectableItems(p *panelState) int {
	if p.Marked == nil {
		p.Marked = map[string]bool{}
	}
	for _, it := range p.Resources {
		markable := false
		if p.Service == "s3" {
			markable = it.Kind == "object" || it.Kind == "bucket"
		} else if p.Service == "rds" {
			markable = it.Kind == "rds-instance" || it.Kind == "rds-cluster"
		} else {
			markable = it.Kind == "generic"
		}
		if !markable {
			continue
		}
		if p.Marked[it.Value] {
			delete(p.Marked, it.Value)
		} else {
			p.Marked[it.Value] = true
		}
	}
	count := 0
	for _, it := range p.Resources {
		if p.Marked[it.Value] {
			count++
		}
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
		roleNote := ""
		if roleCreated {
			roleNote = fmt.Sprintf("created destination role %s", targetRoleArn)
		}

		if exists {
			if err := syncLambdaCode(ctx, job.DstCli, name, srcFn.Code, cfg.Architectures); err != nil {
				return lambdaCopyProgressMsg{Job: job, Name: name, Err: fmt.Errorf("update code: %w", err), Action: action, Note: roleNote}
			}
			upd := buildUpdateLambdaConfigInput(name, cfg, targetRoleArn)
			_, err = job.DstCli.UpdateFunctionConfiguration(ctx, upd)
			if err != nil && isLambdaPassRoleDeniedErr(err) && !job.AutoGrantedRoles[targetRoleArn] {
				if gerr := ensureLambdaPassRoleGrantForJob(ctx, job, targetRoleArn, roleCreated, roleManagedPolicyArns); gerr == nil {
					job.AutoGrantedRoles[targetRoleArn] = true
					_, err = job.DstCli.UpdateFunctionConfiguration(ctx, upd)
				} else {
					return lambdaCopyProgressMsg{Job: job, Name: name, Err: fmt.Errorf("auto-grant failed: %w", gerr), Action: action, Note: roleNote}
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
						roleNote = fmt.Sprintf("created alternative role %s", altRoleArn)
						upd = buildUpdateLambdaConfigInput(name, cfg, targetRoleArn)
						_, err = job.DstCli.UpdateFunctionConfiguration(ctx, upd)
					}
				}
			}
			if err != nil {
				return lambdaCopyProgressMsg{Job: job, Name: name, Err: fmt.Errorf("update config: %w", err), Action: action, Note: roleNote}
			}
			return lambdaCopyProgressMsg{Job: job, Name: name, Action: action, Note: roleNote}
		}

		createInput, err := buildCreateLambdaInput(name, cfg, srcFn.Code, targetRoleArn)
		if err != nil {
			return lambdaCopyProgressMsg{Job: job, Name: name, Err: fmt.Errorf("prepare create: %w", err), Action: action, Note: roleNote}
		}
		_, err = job.DstCli.CreateFunction(ctx, createInput)
		if err != nil && isLambdaPassRoleDeniedErr(err) && !job.AutoGrantedRoles[targetRoleArn] {
			if gerr := ensureLambdaPassRoleGrantForJob(ctx, job, targetRoleArn, roleCreated, roleManagedPolicyArns); gerr == nil {
				job.AutoGrantedRoles[targetRoleArn] = true
				_, err = job.DstCli.CreateFunction(ctx, createInput)
			} else {
				return lambdaCopyProgressMsg{Job: job, Name: name, Err: fmt.Errorf("auto-grant failed: %w", gerr), Action: action, Note: roleNote}
			}
		}
		if err != nil && isLambdaRoleCannotBeAssumedErr(err) {
			if !roleCreated {
				if altRoleArn, altManaged, aerr := createAlternativeDestinationLambdaRole(ctx, job.Src, job.Dst, *cfg.Role, name); aerr == nil {
					targetRoleArn = altRoleArn
					roleCreated = true
					roleManagedPolicyArns = altManaged
					roleNote = fmt.Sprintf("created alternative role %s", altRoleArn)
					createInput, _ = buildCreateLambdaInput(name, cfg, srcFn.Code, targetRoleArn)
					_, err = job.DstCli.CreateFunction(ctx, createInput)
				}
			}
			// Newly-created role needs IAM propagation time. Retry up to 3x with 5s backoff.
			for i := 0; i < 3 && roleCreated && isLambdaRoleCannotBeAssumedErr(err); i++ {
				time.Sleep(5 * time.Second)
				_, err = job.DstCli.CreateFunction(ctx, createInput)
			}
			if err != nil && isLambdaPassRoleDeniedErr(err) && !job.AutoGrantedRoles[targetRoleArn] {
				if gerr := ensureLambdaPassRoleGrantForJob(ctx, job, targetRoleArn, roleCreated, roleManagedPolicyArns); gerr == nil {
					job.AutoGrantedRoles[targetRoleArn] = true
					_, err = job.DstCli.CreateFunction(ctx, createInput)
				}
			}
		}
		if err != nil {
			return lambdaCopyProgressMsg{Job: job, Name: name, Err: fmt.Errorf("create: %w", err), Action: action, Note: roleNote}
		}
		return lambdaCopyProgressMsg{Job: job, Name: name, Action: action, Note: roleNote}
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

// splitTrimComma splits a comma-separated string and trims whitespace from each element.
func splitTrimComma(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
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

// ── API Gateway copy ──────────────────────────────────────────────────────────

func preflightApigwCopyCmd(src, dst panelState, apiIds []string) tea.Cmd {
	return func() tea.Msg {
		action := fmt.Sprintf("check apigateway copy conflicts %s/%s -> %s/%s", src.Profile, src.Region, dst.Profile, dst.Region)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		existing, err := existingApigwNames(ctx, src, dst, apiIds)
		return apigwCopyPreflightMsg{Src: src, Dst: dst, ApiIds: apiIds, Existing: existing, Err: err, Action: action}
	}
}

// existingApigwNames fetches the names of the source APIs, then checks which
// of those names already exist in the destination account. Returns name→true.
func existingApigwNames(ctx context.Context, src, dst panelState, srcApiIds []string) (map[string]bool, error) {
	srcCfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(resolveProfileCase(src.Profile)),
		config.WithRegion(src.Region),
	)
	if err != nil {
		return nil, err
	}
	dstCfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(resolveProfileCase(dst.Profile)),
		config.WithRegion(dst.Region),
	)
	if err != nil {
		return nil, err
	}
	srcCli := apigatewayv2.NewFromConfig(srcCfg)
	dstCli := apigatewayv2.NewFromConfig(dstCfg)

	srcNames := map[string]bool{}
	for _, id := range srcApiIds {
		api, err := srcCli.GetApi(ctx, &apigatewayv2.GetApiInput{ApiId: aws.String(id)})
		if err != nil {
			return nil, fmt.Errorf("read source api %s: %w", id, err)
		}
		if api.Name != nil {
			srcNames[*api.Name] = true
		}
	}
	dstOut, err := dstCli.GetApis(ctx, &apigatewayv2.GetApisInput{})
	if err != nil {
		return nil, fmt.Errorf("list destination apis: %w", err)
	}
	existing := map[string]bool{}
	for _, api := range dstOut.Items {
		if api.Name != nil && srcNames[*api.Name] {
			existing[*api.Name] = true
		}
	}
	return existing, nil
}

func (m *model) startApigwCopyJob(src, dst panelState, apiIds []string, skipExisting bool, existing map[string]bool) tea.Cmd {
	if len(apiIds) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	srcCfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(resolveProfileCase(src.Profile)),
		config.WithRegion(src.Region),
	)
	if err != nil {
		m.status = "AWS config error (apigateway source): " + firstLine(err.Error())
		m.lastInfo = err.Error()
		m.pushLog("ERROR start apigateway copy job :: " + err.Error())
		return nil
	}
	dstCfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(resolveProfileCase(dst.Profile)),
		config.WithRegion(dst.Region),
	)
	if err != nil {
		m.status = "AWS config error (apigateway destination): " + firstLine(err.Error())
		m.lastInfo = err.Error()
		m.pushLog("ERROR start apigateway copy job :: " + err.Error())
		return nil
	}
	if existing == nil {
		existing = map[string]bool{}
	}
	job := apigwCopyJobState{
		Src:          src,
		Dst:          dst,
		ApiIds:       append([]string(nil), apiIds...),
		SkipExisting: skipExisting,
		Existing:     existing,
		SrcCli:       apigatewayv2.NewFromConfig(srcCfg),
		DstCli:       apigatewayv2.NewFromConfig(dstCfg),
	}
	m.apigwJob = &job
	m.status = fmt.Sprintf("Copying API Gateway 1/%d", len(apiIds))
	m.pushLog(fmt.Sprintf("START copy apigateway %s/%s -> %s/%s (%d) [skip_existing=%t]", src.Profile, src.Region, dst.Profile, dst.Region, len(apiIds), skipExisting))
	return apigwCopyStepCmd(job)
}

func apigwCopyStepCmd(job apigwCopyJobState) tea.Cmd {
	return func() tea.Msg {
		if job.Index < 0 || job.Index >= len(job.ApiIds) {
			return apigwCopyProgressMsg{Job: job, Err: fmt.Errorf("index out of range"), Action: "copy apigateway"}
		}
		srcApiId := job.ApiIds[job.Index]
		action := "copy apigateway"
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		srcApi, err := job.SrcCli.GetApi(ctx, &apigatewayv2.GetApiInput{ApiId: aws.String(srcApiId)})
		if err != nil {
			return apigwCopyProgressMsg{Job: job, Err: fmt.Errorf("read source api: %w", err), Action: action}
		}
		apiName := aws.ToString(srcApi.Name)

		if job.Existing[apiName] && job.SkipExisting {
			return apigwCopyProgressMsg{Job: job, ApiName: apiName, Skipped: true, Action: action}
		}

		// Overwrite: delete the existing destination API by name.
		if job.Existing[apiName] {
			if dstOut, err := job.DstCli.GetApis(ctx, &apigatewayv2.GetApisInput{}); err == nil {
				for _, a := range dstOut.Items {
					if aws.ToString(a.Name) == apiName && a.ApiId != nil {
						_, _ = job.DstCli.DeleteApi(ctx, &apigatewayv2.DeleteApiInput{ApiId: a.ApiId})
						break
					}
				}
			}
		}

		// Create API in destination.
		createIn := &apigatewayv2.CreateApiInput{
			Name:                     srcApi.Name,
			ProtocolType:             srcApi.ProtocolType,
			Description:              srcApi.Description,
			RouteSelectionExpression: srcApi.RouteSelectionExpression,
			Tags:                     filterManagedTags(srcApi.Tags),
		}
		if srcApi.CorsConfiguration != nil {
			createIn.CorsConfiguration = srcApi.CorsConfiguration
		}
		dstApi, err := job.DstCli.CreateApi(ctx, createIn)
		if err != nil {
			return apigwCopyProgressMsg{Job: job, ApiName: apiName, Err: fmt.Errorf("create api: %w", err), Action: action}
		}
		dstApiId := aws.ToString(dstApi.ApiId)

		integrationMap, err := copyApigwIntegrations(ctx, job.SrcCli, job.DstCli, srcApiId, dstApiId)
		if err != nil {
			return apigwCopyProgressMsg{Job: job, ApiName: apiName, Err: fmt.Errorf("copy integrations: %w", err), Action: action}
		}
		authorizerMap, err := copyApigwAuthorizers(ctx, job.SrcCli, job.DstCli, srcApiId, dstApiId)
		if err != nil {
			return apigwCopyProgressMsg{Job: job, ApiName: apiName, Err: fmt.Errorf("copy authorizers: %w", err), Action: action}
		}
		if err := copyApigwRoutes(ctx, job.SrcCli, job.DstCli, srcApiId, dstApiId, integrationMap, authorizerMap); err != nil {
			return apigwCopyProgressMsg{Job: job, ApiName: apiName, Err: fmt.Errorf("copy routes: %w", err), Action: action}
		}
		if err := copyApigwStages(ctx, job.SrcCli, job.DstCli, srcApiId, dstApiId); err != nil {
			return apigwCopyProgressMsg{Job: job, ApiName: apiName, Err: fmt.Errorf("copy stages: %w", err), Action: action}
		}
		return apigwCopyProgressMsg{Job: job, ApiName: apiName, Action: action}
	}
}

func copyApigwIntegrations(ctx context.Context, src, dst *apigatewayv2.Client, srcApiId, dstApiId string) (map[string]string, error) {
	out, err := src.GetIntegrations(ctx, &apigatewayv2.GetIntegrationsInput{ApiId: aws.String(srcApiId)})
	if err != nil {
		return nil, err
	}
	idMap := map[string]string{}
	for _, integ := range out.Items {
		if integ.IntegrationId == nil {
			continue
		}
		in := &apigatewayv2.CreateIntegrationInput{
			ApiId:                aws.String(dstApiId),
			IntegrationType:      integ.IntegrationType,
			IntegrationUri:       integ.IntegrationUri,
			IntegrationMethod:    integ.IntegrationMethod,
			PayloadFormatVersion: integ.PayloadFormatVersion,
			TimeoutInMillis:      integ.TimeoutInMillis,
			Description:          integ.Description,
			ConnectionType:       integ.ConnectionType,
		}
		created, err := dst.CreateIntegration(ctx, in)
		if err != nil {
			return nil, fmt.Errorf("create integration: %w", err)
		}
		if created.IntegrationId != nil {
			idMap[*integ.IntegrationId] = *created.IntegrationId
		}
	}
	return idMap, nil
}

func copyApigwAuthorizers(ctx context.Context, src, dst *apigatewayv2.Client, srcApiId, dstApiId string) (map[string]string, error) {
	out, err := src.GetAuthorizers(ctx, &apigatewayv2.GetAuthorizersInput{ApiId: aws.String(srcApiId)})
	if err != nil {
		return nil, err
	}
	idMap := map[string]string{}
	for _, auth := range out.Items {
		if auth.AuthorizerId == nil {
			continue
		}
		in := &apigatewayv2.CreateAuthorizerInput{
			ApiId:                        aws.String(dstApiId),
			AuthorizerType:               auth.AuthorizerType,
			Name:                         auth.Name,
			IdentitySource:               auth.IdentitySource,
			JwtConfiguration:             auth.JwtConfiguration,
			AuthorizerUri:                auth.AuthorizerUri,
			AuthorizerResultTtlInSeconds: auth.AuthorizerResultTtlInSeconds,
			EnableSimpleResponses:        auth.EnableSimpleResponses,
		}
		created, err := dst.CreateAuthorizer(ctx, in)
		if err != nil {
			return nil, fmt.Errorf("create authorizer: %w", err)
		}
		if created.AuthorizerId != nil {
			idMap[*auth.AuthorizerId] = *created.AuthorizerId
		}
	}
	return idMap, nil
}

func copyApigwRoutes(ctx context.Context, src, dst *apigatewayv2.Client, srcApiId, dstApiId string, integrationMap, authorizerMap map[string]string) error {
	out, err := src.GetRoutes(ctx, &apigatewayv2.GetRoutesInput{ApiId: aws.String(srcApiId)})
	if err != nil {
		return err
	}
	for _, route := range out.Items {
		in := &apigatewayv2.CreateRouteInput{
			ApiId:             aws.String(dstApiId),
			RouteKey:          route.RouteKey,
			ApiKeyRequired:    route.ApiKeyRequired,
			AuthorizationType: route.AuthorizationType,
		}
		if len(route.AuthorizationScopes) > 0 {
			in.AuthorizationScopes = route.AuthorizationScopes
		}
		if route.AuthorizerId != nil {
			if dstAuthId, ok := authorizerMap[*route.AuthorizerId]; ok {
				in.AuthorizerId = aws.String(dstAuthId)
			}
		}
		if route.Target != nil {
			t := *route.Target
			if strings.HasPrefix(t, "integrations/") {
				srcIntegId := strings.TrimPrefix(t, "integrations/")
				if dstIntegId, ok := integrationMap[srcIntegId]; ok {
					in.Target = aws.String("integrations/" + dstIntegId)
				} else {
					in.Target = aws.String(t)
				}
			} else {
				in.Target = aws.String(t)
			}
		}
		if _, err := dst.CreateRoute(ctx, in); err != nil {
			return fmt.Errorf("create route %s: %w", aws.ToString(route.RouteKey), err)
		}
	}
	return nil
}

// filterManagedTags removes AWS-reserved tag keys (prefixed with "aws:") that
// cannot be set by users and would cause a 400 BadRequestException on creation.
func filterManagedTags(tags map[string]string) map[string]string {
	out := make(map[string]string, len(tags))
	for k, v := range tags {
		if !strings.HasPrefix(strings.ToLower(k), "aws:") {
			out[k] = v
		}
	}
	return out
}

func copyApigwStages(ctx context.Context, src, dst *apigatewayv2.Client, srcApiId, dstApiId string) error {
	out, err := src.GetStages(ctx, &apigatewayv2.GetStagesInput{ApiId: aws.String(srcApiId)})
	if err != nil {
		return err
	}
	for _, stage := range out.Items {
		if stage.StageName == nil {
			continue
		}
		in := &apigatewayv2.CreateStageInput{
			ApiId:       aws.String(dstApiId),
			StageName:   stage.StageName,
			AutoDeploy:  stage.AutoDeploy,
			Description: stage.Description,
			Tags:        stage.Tags,
		}
		if _, err := dst.CreateStage(ctx, in); err != nil {
			s := strings.ToLower(err.Error())
			// $default stage may be auto-created by AWS; ignore "already exists" conflicts.
			if !strings.Contains(s, "already exists") && !strings.Contains(s, "conflict") {
				return fmt.Errorf("create stage %s: %w", *stage.StageName, err)
			}
		}
	}
	return nil
}

// ─── ECS Copy ────────────────────────────────────────────────────────────────

func preflightEcsCopyCmd(src, dst panelState, clusterArns []string) tea.Cmd {
	return func() tea.Msg {
		action := fmt.Sprintf("preflight ecs copy %s/%s -> %s/%s (%d)", src.Profile, src.Region, dst.Profile, dst.Region, len(clusterArns))
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		cfg, err := config.LoadDefaultConfig(ctx,
			config.WithSharedConfigProfile(resolveProfileCase(dst.Profile)),
			config.WithRegion(dst.Region),
		)
		if err != nil {
			return ecsCopyPreflightMsg{Src: src, Dst: dst, Clusters: clusterArns, Err: err, Action: action}
		}
		cli := ecs.NewFromConfig(cfg)
		// Build set of destination cluster names.
		dstNames := map[string]bool{}
		var token *string
		for {
			out, err := cli.ListClusters(ctx, &ecs.ListClustersInput{NextToken: token})
			if err != nil {
				break
			}
			for _, arn := range out.ClusterArns {
				name := arn
				if idx := strings.LastIndex(arn, "/"); idx >= 0 && idx+1 < len(arn) {
					name = arn[idx+1:]
				}
				dstNames[name] = true
			}
			if out.NextToken == nil || *out.NextToken == "" {
				break
			}
			token = out.NextToken
		}
		// Check each source cluster name against destination.
		existing := map[string]bool{}
		for _, arn := range clusterArns {
			name := arn
			if idx := strings.LastIndex(arn, "/"); idx >= 0 && idx+1 < len(arn) {
				name = arn[idx+1:]
			}
			if dstNames[name] {
				existing[name] = true
			}
		}
		return ecsCopyPreflightMsg{Src: src, Dst: dst, Clusters: clusterArns, Existing: existing, Action: action}
	}
}

func (m *model) startEcsCopyJob(src, dst panelState, clusterArns []string, skipExisting bool, existing map[string]bool) tea.Cmd {
	if len(clusterArns) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	srcCfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(resolveProfileCase(src.Profile)),
		config.WithRegion(src.Region),
	)
	if err != nil {
		m.status = "AWS config error (ecs source): " + firstLine(err.Error())
		m.lastInfo = err.Error()
		m.pushLog("ERROR start ecs copy job :: " + err.Error())
		return nil
	}
	dstCfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(resolveProfileCase(dst.Profile)),
		config.WithRegion(dst.Region),
	)
	if err != nil {
		m.status = "AWS config error (ecs destination): " + firstLine(err.Error())
		m.lastInfo = err.Error()
		m.pushLog("ERROR start ecs copy job :: " + err.Error())
		return nil
	}
	if existing == nil {
		existing = map[string]bool{}
	}
	job := ecsCopyJobState{
		Src:          src,
		Dst:          dst,
		Clusters:     append([]string(nil), clusterArns...),
		SkipExisting: skipExisting,
		Existing:     existing,
		SrcCli:       ecs.NewFromConfig(srcCfg),
		DstCli:       ecs.NewFromConfig(dstCfg),
	}
	m.ecsCopyJob = &job
	m.status = fmt.Sprintf("Copying ECS cluster 1/%d", len(clusterArns))
	m.pushLog(fmt.Sprintf("START copy ecs %s/%s -> %s/%s (%d) [skip_existing=%t]", src.Profile, src.Region, dst.Profile, dst.Region, len(clusterArns), skipExisting))
	return ecsCopyStepCmd(job)
}

func ecsCopyStepCmd(job ecsCopyJobState) tea.Cmd {
	return func() tea.Msg {
		if job.Index < 0 || job.Index >= len(job.Clusters) {
			return ecsCopyProgressMsg{Job: job, Err: fmt.Errorf("index out of range"), Action: "copy ecs"}
		}
		srcArn := job.Clusters[job.Index]
		action := "copy ecs"
		// Extract cluster name from ARN.
		clusterName := srcArn
		if idx := strings.LastIndex(srcArn, "/"); idx >= 0 && idx+1 < len(srcArn) {
			clusterName = srcArn[idx+1:]
		}
		// Skip if requested and cluster exists in destination.
		if job.SkipExisting && job.Existing[clusterName] {
			return ecsCopyProgressMsg{Job: job, ClusterName: clusterName, Skipped: true, Action: action}
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		// Get source cluster details.
		srcOut, err := job.SrcCli.DescribeClusters(ctx, &ecs.DescribeClustersInput{
			Clusters: []string{srcArn},
			Include:  []ecstypes.ClusterField{"SETTINGS", "CONFIGURATIONS"},
		})
		if err != nil {
			return ecsCopyProgressMsg{Job: job, ClusterName: clusterName, Err: fmt.Errorf("describe source cluster: %w", err), Action: action}
		}
		if len(srcOut.Clusters) == 0 {
			return ecsCopyProgressMsg{Job: job, ClusterName: clusterName, Err: fmt.Errorf("source cluster not found"), Action: action}
		}
		src := srcOut.Clusters[0]
		// Build CreateCluster input from source cluster.
		in := &ecs.CreateClusterInput{
			ClusterName: aws.String(clusterName),
		}
		if len(src.Settings) > 0 {
			in.Settings = src.Settings
		}
		if src.Configuration != nil {
			in.Configuration = src.Configuration
		}
		if len(src.Tags) > 0 {
			in.Tags = src.Tags
		}
		_, err = job.DstCli.CreateCluster(ctx, in)
		if err != nil {
			// ECS CreateCluster is effectively idempotent when the cluster already exists —
			// treat any "already exists" response as a successful overwrite.
			s := strings.ToLower(err.Error())
			if !strings.Contains(s, "already exists") && !strings.Contains(s, "conflict") {
				return ecsCopyProgressMsg{Job: job, ClusterName: clusterName, Err: fmt.Errorf("create cluster: %w", err), Action: action}
			}
		}
		return ecsCopyProgressMsg{Job: job, ClusterName: clusterName, Action: action}
	}
}

// ─── ECR Copy ────────────────────────────────────────────────────────────────

func preflightEcrCopyCmd(src, dst panelState, repos []string) tea.Cmd {
	return func() tea.Msg {
		action := fmt.Sprintf("preflight ecr copy %s/%s -> %s/%s (%d)", src.Profile, src.Region, dst.Profile, dst.Region, len(repos))
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		cfg, err := config.LoadDefaultConfig(ctx,
			config.WithSharedConfigProfile(resolveProfileCase(dst.Profile)),
			config.WithRegion(dst.Region),
		)
		if err != nil {
			return ecrCopyPreflightMsg{Src: src, Dst: dst, Repos: repos, Err: err, Action: action}
		}
		cli := ecr.NewFromConfig(cfg)
		// Get existing destination repositories.
		dstRepos := map[string]bool{}
		var token *string
		for {
			out, err := cli.DescribeRepositories(ctx, &ecr.DescribeRepositoriesInput{NextToken: token})
			if err != nil {
				break
			}
			for _, r := range out.Repositories {
				if r.RepositoryName != nil {
					dstRepos[*r.RepositoryName] = true
				}
			}
			if out.NextToken == nil || *out.NextToken == "" {
				break
			}
			token = out.NextToken
		}
		existing := map[string]bool{}
		for _, r := range repos {
			if dstRepos[r] {
				existing[r] = true
			}
		}
		return ecrCopyPreflightMsg{Src: src, Dst: dst, Repos: repos, Existing: existing, Action: action}
	}
}

// ecrAllDone finalises an ECR copy job: updates status, resets state, and
// triggers a panel refresh.
func (m *model) ecrAllDone(job ecrCopyJobState) tea.Cmd {
	if job.Failed > 0 {
		m.status = fmt.Sprintf("ECR copy finished with errors: ok=%d skip=%d fail=%d", job.Success, job.Skipped, job.Failed)
	} else {
		m.status = fmt.Sprintf("ECR copy completed: ok=%d skip=%d", job.Success, job.Skipped)
	}
	m.lastInfo = m.status
	m.ecrCopyJob = nil
	m.mode = modeNormal
	m.persistContext()
	return tea.Batch(loadPanelCmd(leftPanel, m.left), loadPanelCmd(rightPanel, m.right))
}

func (m *model) startEcrCopyJob(src, dst panelState, repos []string, skipExisting bool, existing map[string]bool) tea.Cmd {
	if len(repos) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	srcCfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(resolveProfileCase(src.Profile)),
		config.WithRegion(src.Region),
	)
	if err != nil {
		m.status = "AWS config error (ecr source): " + firstLine(err.Error())
		m.lastInfo = err.Error()
		m.pushLog("ERROR start ecr copy job :: " + err.Error())
		return nil
	}
	dstCfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(resolveProfileCase(dst.Profile)),
		config.WithRegion(dst.Region),
	)
	if err != nil {
		m.status = "AWS config error (ecr destination): " + firstLine(err.Error())
		m.lastInfo = err.Error()
		m.pushLog("ERROR start ecr copy job :: " + err.Error())
		return nil
	}
	if existing == nil {
		existing = map[string]bool{}
	}
	job := ecrCopyJobState{
		Src:          src,
		Dst:          dst,
		Repos:        append([]string(nil), repos...),
		SkipExisting: skipExisting,
		Existing:     existing,
		SrcCli:       ecr.NewFromConfig(srcCfg),
		DstCli:       ecr.NewFromConfig(dstCfg),
	}
	m.ecrCopyJob = &job
	m.status = fmt.Sprintf("ECR: preparing repo %s (1/%d)...", repos[0], len(repos))
	m.pushLog(fmt.Sprintf("START copy ecr %s/%s -> %s/%s (%d) [skip_existing=%t]", src.Profile, src.Region, dst.Profile, dst.Region, len(repos), skipExisting))
	return ecrPrepRepoCmd(job)
}

// ecrPrepRepoCmd creates the destination repository if needed, lists all source
// images, and returns ecrRepoReadyMsg so the UI can start per-image progress.
func ecrPrepRepoCmd(job ecrCopyJobState) tea.Cmd {
	return func() tea.Msg {
		if job.Index < 0 || job.Index >= len(job.Repos) {
			return ecrCopyProgressMsg{Job: job, Err: fmt.Errorf("index out of range"), Action: "ecr prep repo"}
		}
		repoName := job.Repos[job.Index]
		action := fmt.Sprintf("ecr prep repo %s", repoName)

		if job.SkipExisting && job.Existing[repoName] {
			return ecrCopyProgressMsg{Job: job, RepoName: repoName, Skipped: true, Action: action}
		}

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		// Describe source repo for creation settings.
		srcRepoOut, err := job.SrcCli.DescribeRepositories(ctx, &ecr.DescribeRepositoriesInput{
			RepositoryNames: []string{repoName},
		})
		if err != nil {
			return ecrRepoReadyMsg{Job: job, Err: fmt.Errorf("describe source repo: %w", err), Action: action}
		}
		if len(srcRepoOut.Repositories) == 0 {
			return ecrRepoReadyMsg{Job: job, Err: fmt.Errorf("source repository not found"), Action: action}
		}
		srcRepo := srcRepoOut.Repositories[0]

		// Ensure destination repository exists.
		dstRepoOut, dstErr := job.DstCli.DescribeRepositories(ctx, &ecr.DescribeRepositoriesInput{
			RepositoryNames: []string{repoName},
		})
		if dstErr != nil || len(dstRepoOut.Repositories) == 0 {
			createIn := &ecr.CreateRepositoryInput{
				RepositoryName: aws.String(repoName),
			}
			if srcRepo.ImageTagMutability != "" {
				createIn.ImageTagMutability = srcRepo.ImageTagMutability
			}
			if srcRepo.ImageScanningConfiguration != nil {
				createIn.ImageScanningConfiguration = &ecrtypes.ImageScanningConfiguration{
					ScanOnPush: srcRepo.ImageScanningConfiguration.ScanOnPush,
				}
			}
			if srcRepo.EncryptionConfiguration != nil {
				createIn.EncryptionConfiguration = &ecrtypes.EncryptionConfiguration{
					EncryptionType: srcRepo.EncryptionConfiguration.EncryptionType,
					KmsKey:         srcRepo.EncryptionConfiguration.KmsKey,
				}
			}
			if _, cerr := job.DstCli.CreateRepository(ctx, createIn); cerr != nil {
				return ecrRepoReadyMsg{Job: job, Err: fmt.Errorf("create destination repo: %w", cerr), Action: action}
			}
		}

		// List all image IDs by digest (deduplicated) to avoid processing the same
		// image data twice when it has multiple tags.
		seenDigests := map[string]bool{}
		var images []ecrtypes.ImageIdentifier
		var imgToken *string
		for {
			listOut, lerr := job.SrcCli.ListImages(ctx, &ecr.ListImagesInput{
				RepositoryName: aws.String(repoName),
				NextToken:      imgToken,
				Filter:         &ecrtypes.ListImagesFilter{TagStatus: ecrtypes.TagStatusTagged},
			})
			if lerr != nil {
				return ecrRepoReadyMsg{Job: job, Err: fmt.Errorf("list images: %w", lerr), Action: action}
			}
			for _, id := range listOut.ImageIds {
				d := aws.ToString(id.ImageDigest)
				if d == "" || seenDigests[d] {
					continue
				}
				seenDigests[d] = true
				images = append(images, id)
			}
			if listOut.NextToken == nil || *listOut.NextToken == "" {
				break
			}
			imgToken = listOut.NextToken
		}
		return ecrRepoReadyMsg{Job: job, Images: images, Action: action}
	}
}

// ecrCopyImageCmd copies a single image (job.Images[job.ImageIndex]) from source
// to destination, uploading any missing layers, then putting the manifest.
func ecrCopyImageCmd(job ecrCopyJobState) tea.Cmd {
	return func() tea.Msg {
		repoName := job.Repos[job.Index]
		action := fmt.Sprintf("ecr copy image %s[%d]", repoName, job.ImageIndex)
		imgID := job.Images[job.ImageIndex]
		imageTag := aws.ToString(imgID.ImageTag)
		if imageTag == "" {
			imageTag = aws.ToString(imgID.ImageDigest)
			if len(imageTag) > 19 {
				imageTag = imageTag[:19]
			}
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
		defer cancel()

		// Fetch the manifest for this single image.
		batchOut, berr := job.SrcCli.BatchGetImage(ctx, &ecr.BatchGetImageInput{
			RepositoryName: aws.String(repoName),
			ImageIds:       []ecrtypes.ImageIdentifier{imgID},
			AcceptedMediaTypes: []string{
				"application/vnd.docker.distribution.manifest.v2+json",
				"application/vnd.docker.distribution.manifest.list.v2+json",
				"application/vnd.oci.image.manifest.v1+json",
				"application/vnd.oci.image.index.v1+json",
			},
		})
		if berr != nil {
			return ecrImageProgressMsg{Job: job, RepoName: repoName, ImageTag: imageTag, Err: fmt.Errorf("get manifest: %w", berr), Action: action}
		}
		if len(batchOut.Images) == 0 || batchOut.Images[0].ImageManifest == nil {
			return ecrImageProgressMsg{Job: job, RepoName: repoName, ImageTag: imageTag, Err: fmt.Errorf("manifest not found"), Action: action}
		}
		img := batchOut.Images[0]

		layersCopied := 0

		// Parse manifest to extract layer digests.
		digests, parseErr := extractECRLayerDigests(*img.ImageManifest)
		if parseErr == nil && len(digests) > 0 {
			// Check which layers are missing in destination.
			checkOut, cerr := job.DstCli.BatchCheckLayerAvailability(ctx, &ecr.BatchCheckLayerAvailabilityInput{
				RepositoryName: aws.String(repoName),
				LayerDigests:   digests,
			})
			if cerr != nil {
				return ecrImageProgressMsg{Job: job, RepoName: repoName, ImageTag: imageTag, Err: fmt.Errorf("check layers: %w", cerr), Action: action}
			}
			// Upload each UNAVAILABLE layer.
			for _, layer := range checkOut.Layers {
				if layer.LayerAvailability != ecrtypes.LayerAvailabilityAvailable {
					digest := aws.ToString(layer.LayerDigest)
					if digest == "" {
						continue
					}
					if lerr := copyECRLayer(ctx, job.SrcCli, job.DstCli, repoName, repoName, digest); lerr != nil {
						return ecrImageProgressMsg{Job: job, RepoName: repoName, ImageTag: imageTag, Err: fmt.Errorf("copy layer %s: %w", digest[:min(len(digest), 19)], lerr), Action: action}
					}
					layersCopied++
				}
			}
		}

		// Put the image manifest.
		putIn := &ecr.PutImageInput{
			RepositoryName: aws.String(repoName),
			ImageManifest:  img.ImageManifest,
		}
		if img.ImageId.ImageTag != nil {
			putIn.ImageTag = img.ImageId.ImageTag
		}
		if img.ImageManifestMediaType != nil {
			putIn.ImageManifestMediaType = img.ImageManifestMediaType
		}
		if _, perr := job.DstCli.PutImage(ctx, putIn); perr != nil {
			s := strings.ToLower(perr.Error())
			if !strings.Contains(s, "already exists") && !strings.Contains(s, "imagetagalreadyexists") {
				return ecrImageProgressMsg{Job: job, RepoName: repoName, ImageTag: imageTag, Err: fmt.Errorf("put image: %w", perr), Action: action}
			}
		}
		return ecrImageProgressMsg{Job: job, RepoName: repoName, ImageTag: imageTag, Layers: layersCopied, Action: action}
	}
}

// copyECRLayer downloads a layer from the source ECR repository and uploads it
// to the destination ECR repository using the layer upload API.
func copyECRLayer(ctx context.Context, srcCli, dstCli *ecr.Client, srcRepo, dstRepo, digest string) error {
	// Get pre-signed download URL for the layer.
	urlOut, err := srcCli.GetDownloadUrlForLayer(ctx, &ecr.GetDownloadUrlForLayerInput{
		RepositoryName: aws.String(srcRepo),
		LayerDigest:    aws.String(digest),
	})
	if err != nil {
		return fmt.Errorf("get layer url: %w", err)
	}

	// Download the layer data.
	resp, err := http.Get(aws.ToString(urlOut.DownloadUrl))
	if err != nil {
		return fmt.Errorf("download layer: %w", err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read layer data: %w", err)
	}

	// Initiate the upload in destination.
	initOut, err := dstCli.InitiateLayerUpload(ctx, &ecr.InitiateLayerUploadInput{
		RepositoryName: aws.String(dstRepo),
	})
	if err != nil {
		return fmt.Errorf("initiate layer upload: %w", err)
	}
	uploadID := aws.ToString(initOut.UploadId)

	// Upload in chunks of 5 MiB.
	const chunkSize = 5 * 1024 * 1024
	var offset int64
	for offset < int64(len(data)) {
		end := offset + chunkSize
		if end > int64(len(data)) {
			end = int64(len(data))
		}
		_, uerr := dstCli.UploadLayerPart(ctx, &ecr.UploadLayerPartInput{
			RepositoryName: aws.String(dstRepo),
			UploadId:       aws.String(uploadID),
			PartFirstByte:  aws.Int64(offset),
			PartLastByte:   aws.Int64(end - 1),
			LayerPartBlob:  data[offset:end],
		})
		if uerr != nil {
			return fmt.Errorf("upload layer part: %w", uerr)
		}
		offset = end
	}

	// Complete the upload.
	_, err = dstCli.CompleteLayerUpload(ctx, &ecr.CompleteLayerUploadInput{
		RepositoryName: aws.String(dstRepo),
		UploadId:       aws.String(uploadID),
		LayerDigests:   []string{digest},
	})
	if err != nil {
		return fmt.Errorf("complete layer upload: %w", err)
	}
	return nil
}

// extractECRLayerDigests parses a Docker/OCI image manifest and returns all
// layer digests (including the config blob).
func extractECRLayerDigests(manifest string) ([]string, error) {
	var m struct {
		Config struct {
			Digest string `json:"digest"`
		} `json:"config"`
		Layers []struct {
			Digest string `json:"digest"`
		} `json:"layers"`
	}
	if err := json.Unmarshal([]byte(manifest), &m); err != nil {
		return nil, err
	}
	digests := make([]string, 0)
	if m.Config.Digest != "" {
		digests = append(digests, m.Config.Digest)
	}
	for _, l := range m.Layers {
		if l.Digest != "" {
			digests = append(digests, l.Digest)
		}
	}
	return digests, nil
}

// ─── ECS Service Copy ─────────────────────────────────────────────────────────

func preflightEcsSvcCopyCmd(src, dst panelState, serviceArns []string) tea.Cmd {
	return func() tea.Msg {
		action := fmt.Sprintf("preflight ecs service copy %s/%s/%s -> %s/%s/%s (%d)",
			src.Profile, src.Region, src.S3Bucket, dst.Profile, dst.Region, dst.S3Bucket, len(serviceArns))
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		cfg, err := config.LoadDefaultConfig(ctx,
			config.WithSharedConfigProfile(resolveProfileCase(dst.Profile)),
			config.WithRegion(dst.Region),
		)
		if err != nil {
			return ecsSvcCopyPreflightMsg{Src: src, Dst: dst, ServiceArns: serviceArns, Err: err, Action: action}
		}
		cli := ecs.NewFromConfig(cfg)
		// List services already in destination cluster.
		dstNames := map[string]bool{}
		var token *string
		for {
			out, lerr := cli.ListServices(ctx, &ecs.ListServicesInput{Cluster: aws.String(dst.S3Bucket), NextToken: token})
			if lerr != nil {
				break
			}
			for _, arn := range out.ServiceArns {
				name := arn
				if idx := strings.LastIndex(arn, "/"); idx >= 0 && idx+1 < len(arn) {
					name = arn[idx+1:]
				}
				dstNames[name] = true
			}
			if out.NextToken == nil || *out.NextToken == "" {
				break
			}
			token = out.NextToken
		}
		existing := map[string]bool{}
		for _, arn := range serviceArns {
			name := arn
			if idx := strings.LastIndex(arn, "/"); idx >= 0 && idx+1 < len(arn) {
				name = arn[idx+1:]
			}
			if dstNames[name] {
				existing[name] = true
			}
		}

		// Check if any source services use awsvpc mode (FARGATE) and accounts/regions differ.
		// If so, subnet/SG IDs won't transfer — try to auto-detect, fall back to asking.
		needsNetworkConfig := false
		var autoDstSubnets, autoDstSecGroups []string
		var autoNetworkMsg string
		srcAccountID := arnAccountID(src.S3Bucket)
		dstAccountID := arnAccountID(dst.S3Bucket)
		crossEnv := srcAccountID != dstAccountID || src.Region != dst.Region
		var awsvpcServices []ecstypes.Service
		if crossEnv && len(serviceArns) > 0 {
			srcCfg, cerr := config.LoadDefaultConfig(ctx,
				config.WithSharedConfigProfile(resolveProfileCase(src.Profile)),
				config.WithRegion(src.Region),
			)
			if cerr == nil {
				srcCli := ecs.NewFromConfig(srcCfg)
				desc, derr := srcCli.DescribeServices(ctx, &ecs.DescribeServicesInput{
					Cluster:  aws.String(src.S3Bucket),
					Services: serviceArns,
				})
				if derr == nil {
					for _, svc := range desc.Services {
						if svc.NetworkConfiguration != nil && svc.NetworkConfiguration.AwsvpcConfiguration != nil {
							needsNetworkConfig = true
							awsvpcServices = append(awsvpcServices, svc)
						}
					}
				}
			}
		}
		if needsNetworkConfig {
			dstCfg2, cerr := config.LoadDefaultConfig(ctx,
				config.WithSharedConfigProfile(resolveProfileCase(dst.Profile)),
				config.WithRegion(dst.Region),
			)
			srcCfg2, cerr2 := config.LoadDefaultConfig(ctx,
				config.WithSharedConfigProfile(resolveProfileCase(src.Profile)),
				config.WithRegion(src.Region),
			)
			if cerr == nil && cerr2 == nil {
				detectedSubnets, detectedSGs, netMsg, derr := autoDetectDestinationNetwork(ctx, srcCfg2, dstCfg2, awsvpcServices)
				if derr == nil && len(detectedSubnets) > 0 {
					autoDstSubnets = detectedSubnets
					autoDstSecGroups = detectedSGs
					autoNetworkMsg = netMsg
					needsNetworkConfig = false
				}
			}
		}
		return ecsSvcCopyPreflightMsg{
			Src: src, Dst: dst, ServiceArns: serviceArns, Existing: existing,
			NeedsNetworkConfig: needsNetworkConfig,
			AutoDstSubnets:     autoDstSubnets,
			AutoDstSecGroups:   autoDstSecGroups,
			AutoNetworkMsg:     autoNetworkMsg,
			Action:             action,
		}
	}
}

func (m *model) startEcsSvcCopyJob(src, dst panelState, serviceArns []string, skipExisting bool, existing map[string]bool, dstSubnets, dstSecurityGroups []string) tea.Cmd {
	if len(serviceArns) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	srcCfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(resolveProfileCase(src.Profile)),
		config.WithRegion(src.Region),
	)
	if err != nil {
		m.status = "AWS config error (ecs service source): " + firstLine(err.Error())
		m.lastInfo = err.Error()
		m.pushLog("ERROR start ecs service copy job :: " + err.Error())
		return nil
	}
	dstCfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(resolveProfileCase(dst.Profile)),
		config.WithRegion(dst.Region),
	)
	if err != nil {
		m.status = "AWS config error (ecs service destination): " + firstLine(err.Error())
		m.lastInfo = err.Error()
		m.pushLog("ERROR start ecs service copy job :: " + err.Error())
		return nil
	}
	if existing == nil {
		existing = map[string]bool{}
	}
	job := ecsSvcCopyJobState{
		Src:               src,
		Dst:               dst,
		ServiceArns:       append([]string(nil), serviceArns...),
		SkipExisting:      skipExisting,
		Existing:          existing,
		DstSubnets:        dstSubnets,
		DstSecurityGroups: dstSecurityGroups,
		SrcCli:            ecs.NewFromConfig(srcCfg),
		DstCli:            ecs.NewFromConfig(dstCfg),
	}
	m.ecsSvcCopyJob = &job
	m.status = fmt.Sprintf("Copying ECS service 1/%d", len(serviceArns))
	m.pushLog(fmt.Sprintf("START copy ecs services %s/%s/%s -> %s/%s/%s (%d) [skip_existing=%t]",
		src.Profile, src.Region, src.S3Bucket, dst.Profile, dst.Region, dst.S3Bucket, len(serviceArns), skipExisting))
	return ecsSvcCopyStepCmd(job)
}

func ecsSvcCopyStepCmd(job ecsSvcCopyJobState) tea.Cmd {
	return func() tea.Msg {
		if job.Index < 0 || job.Index >= len(job.ServiceArns) {
			return ecsSvcCopyProgressMsg{Job: job, Err: fmt.Errorf("index out of range"), Action: "copy ecs service"}
		}
		srcArn := job.ServiceArns[job.Index]
		action := "copy ecs service"

		// Extract service name from ARN.
		svcName := srcArn
		if idx := strings.LastIndex(srcArn, "/"); idx >= 0 && idx+1 < len(srcArn) {
			svcName = srcArn[idx+1:]
		}

		// Skip if requested.
		if job.SkipExisting && job.Existing[svcName] {
			return ecsSvcCopyProgressMsg{Job: job, ServiceName: svcName, Skipped: true, Action: action}
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		// Get source service details.
		descOut, err := job.SrcCli.DescribeServices(ctx, &ecs.DescribeServicesInput{
			Cluster:  aws.String(job.Src.S3Bucket),
			Services: []string{srcArn},
		})
		if err != nil {
			return ecsSvcCopyProgressMsg{Job: job, ServiceName: svcName, Err: fmt.Errorf("describe source service: %w", err), Action: action}
		}
		if len(descOut.Services) == 0 {
			return ecsSvcCopyProgressMsg{Job: job, ServiceName: svcName, Err: fmt.Errorf("source service not found"), Action: action}
		}
		src := descOut.Services[0]

		// Resolve task definition: extract family[:revision] from the ARN.
		taskDef := aws.ToString(src.TaskDefinition)
		if strings.Contains(taskDef, "/") {
			taskDef = taskDef[strings.LastIndex(taskDef, "/")+1:]
		}

		// Copy the task definition to the destination account, replacing account IDs.
		srcAccountID := arnAccountID(job.Src.S3Bucket)
		dstAccountID := arnAccountID(job.Dst.S3Bucket)
		if copiedDef, tdErr := copyECSTaskDefinition(ctx, job.SrcCli, job.DstCli, taskDef, srcAccountID, dstAccountID); tdErr == nil {
			taskDef = copiedDef
		}

		in := &ecs.CreateServiceInput{
			Cluster:                       aws.String(job.Dst.S3Bucket),
			ServiceName:                   aws.String(svcName),
			TaskDefinition:                aws.String(taskDef),
			DesiredCount:                  aws.Int32(src.DesiredCount),
			LaunchType:                    src.LaunchType,
			SchedulingStrategy:            src.SchedulingStrategy,
			EnableExecuteCommand:          src.EnableExecuteCommand,
			EnableECSManagedTags:          src.EnableECSManagedTags,
			PropagateTags:                 src.PropagateTags,
			PlatformVersion:               src.PlatformVersion,
		}
		if len(job.DstSubnets) > 0 {
			// Use the user-supplied network config (cross-account copy).
			assign := ecstypes.AssignPublicIp("DISABLED")
			if src.NetworkConfiguration != nil && src.NetworkConfiguration.AwsvpcConfiguration != nil {
				assign = src.NetworkConfiguration.AwsvpcConfiguration.AssignPublicIp
			}
			in.NetworkConfiguration = &ecstypes.NetworkConfiguration{
				AwsvpcConfiguration: &ecstypes.AwsVpcConfiguration{
					Subnets:        job.DstSubnets,
					SecurityGroups: job.DstSecurityGroups,
					AssignPublicIp: assign,
				},
			}
		} else if src.NetworkConfiguration != nil {
			in.NetworkConfiguration = src.NetworkConfiguration
		}
		if src.DeploymentConfiguration != nil {
			in.DeploymentConfiguration = src.DeploymentConfiguration
		}
		if len(src.CapacityProviderStrategy) > 0 {
			in.CapacityProviderStrategy = src.CapacityProviderStrategy
		}
		if len(src.PlacementConstraints) > 0 {
			in.PlacementConstraints = src.PlacementConstraints
		}
		if len(src.PlacementStrategy) > 0 {
			in.PlacementStrategy = src.PlacementStrategy
		}
		if len(src.ServiceRegistries) > 0 {
			in.ServiceRegistries = src.ServiceRegistries
		}
		if len(src.Tags) > 0 {
			in.Tags = src.Tags
		}

		// If overwrite mode and service exists, update instead of create.
		if job.Existing[svcName] && !job.SkipExisting {
			updateIn := &ecs.UpdateServiceInput{
				Cluster:              aws.String(job.Dst.S3Bucket),
				Service:              aws.String(svcName),
				TaskDefinition:       aws.String(taskDef),
				DesiredCount:         aws.Int32(src.DesiredCount),
				EnableExecuteCommand: aws.Bool(src.EnableExecuteCommand),
				PlatformVersion:      src.PlatformVersion,
			}
			if src.NetworkConfiguration != nil {
				updateIn.NetworkConfiguration = src.NetworkConfiguration
			}
			if src.DeploymentConfiguration != nil {
				updateIn.DeploymentConfiguration = src.DeploymentConfiguration
			}
			if len(src.CapacityProviderStrategy) > 0 {
				updateIn.CapacityProviderStrategy = src.CapacityProviderStrategy
			}
			if _, uerr := job.DstCli.UpdateService(ctx, updateIn); uerr != nil {
				return ecsSvcCopyProgressMsg{Job: job, ServiceName: svcName, Err: fmt.Errorf("update service: %w", uerr), Action: action}
			}
			return ecsSvcCopyProgressMsg{Job: job, ServiceName: svcName, Action: action}
		}

		if _, cerr := job.DstCli.CreateService(ctx, in); cerr != nil {
			return ecsSvcCopyProgressMsg{Job: job, ServiceName: svcName, Err: fmt.Errorf("create service: %w", cerr), Action: action}
		}
		return ecsSvcCopyProgressMsg{Job: job, ServiceName: svcName, Action: action}
	}
}

// autoDetectDestinationNetwork attempts to find suitable subnets and security groups
// in the destination account by matching the source VPC name tag, then falling back
// to the default VPC. Returns subnets, security groups, a description message, and
// an error if auto-detection fails.
func autoDetectDestinationNetwork(ctx context.Context, srcCfg, dstCfg aws.Config, services []ecstypes.Service) (subnets, secGroups []string, msg string, err error) {
	// Collect all source subnet IDs used by the services.
	srcSubnetSet := map[string]bool{}
	for _, svc := range services {
		if svc.NetworkConfiguration != nil && svc.NetworkConfiguration.AwsvpcConfiguration != nil {
			for _, s := range svc.NetworkConfiguration.AwsvpcConfiguration.Subnets {
				srcSubnetSet[s] = true
			}
		}
	}
	if len(srcSubnetSet) == 0 {
		return nil, nil, "", fmt.Errorf("no awsvpc subnets found in source services")
	}
	srcSubnetIDs := make([]string, 0, len(srcSubnetSet))
	for s := range srcSubnetSet {
		srcSubnetIDs = append(srcSubnetIDs, s)
	}

	srcEC2 := ec2.NewFromConfig(srcCfg)
	dstEC2 := ec2.NewFromConfig(dstCfg)

	// Describe source subnets to get VPC ID, AZs, and VPC Name tag.
	srcSubDesc, serr := srcEC2.DescribeSubnets(ctx, &ec2.DescribeSubnetsInput{
		SubnetIds: srcSubnetIDs,
	})
	if serr != nil {
		return nil, nil, "", fmt.Errorf("describe source subnets: %w", serr)
	}
	if len(srcSubDesc.Subnets) == 0 {
		return nil, nil, "", fmt.Errorf("no source subnet details returned")
	}

	// Collect AZs and source VPC ID.
	srcVpcID := aws.ToString(srcSubDesc.Subnets[0].VpcId)
	srcAZSet := map[string]bool{}
	for _, sub := range srcSubDesc.Subnets {
		srcAZSet[aws.ToString(sub.AvailabilityZone)] = true
	}

	// Find source VPC name tag.
	srcVpcName := ""
	srcVpcDesc, verr := srcEC2.DescribeVpcs(ctx, &ec2.DescribeVpcsInput{
		VpcIds: []string{srcVpcID},
	})
	if verr == nil && len(srcVpcDesc.Vpcs) > 0 {
		for _, tag := range srcVpcDesc.Vpcs[0].Tags {
			if aws.ToString(tag.Key) == "Name" {
				srcVpcName = aws.ToString(tag.Value)
				break
			}
		}
	}

	// Find matching VPC in destination by Name tag, then fall back to default VPC.
	dstVpcID := ""
	matchedBy := ""
	if srcVpcName != "" {
		vpcOut, verr2 := dstEC2.DescribeVpcs(ctx, &ec2.DescribeVpcsInput{
			Filters: []ec2types.Filter{
				{Name: aws.String("tag:Name"), Values: []string{srcVpcName}},
			},
		})
		if verr2 == nil && len(vpcOut.Vpcs) > 0 {
			dstVpcID = aws.ToString(vpcOut.Vpcs[0].VpcId)
			matchedBy = fmt.Sprintf("VPC name=%q", srcVpcName)
		}
	}
	if dstVpcID == "" {
		// Fall back to default VPC.
		vpcOut, verr2 := dstEC2.DescribeVpcs(ctx, &ec2.DescribeVpcsInput{
			Filters: []ec2types.Filter{
				{Name: aws.String("isDefault"), Values: []string{"true"}},
			},
		})
		if verr2 != nil || len(vpcOut.Vpcs) == 0 {
			return nil, nil, "", fmt.Errorf("no matching VPC found in destination (tried name=%q, default VPC)", srcVpcName)
		}
		dstVpcID = aws.ToString(vpcOut.Vpcs[0].VpcId)
		matchedBy = "default VPC"
	}

	// Get destination subnets in the same AZs.
	azList := make([]string, 0, len(srcAZSet))
	for az := range srcAZSet {
		azList = append(azList, az)
	}
	dstSubDesc, serr2 := dstEC2.DescribeSubnets(ctx, &ec2.DescribeSubnetsInput{
		Filters: []ec2types.Filter{
			{Name: aws.String("vpc-id"), Values: []string{dstVpcID}},
			{Name: aws.String("availabilityZone"), Values: azList},
		},
	})
	if serr2 != nil {
		return nil, nil, "", fmt.Errorf("describe destination subnets: %w", serr2)
	}

	// Prefer one subnet per AZ (first found).
	seenAZ := map[string]bool{}
	var dstSubnets []string
	for _, sub := range dstSubDesc.Subnets {
		az := aws.ToString(sub.AvailabilityZone)
		if !seenAZ[az] {
			dstSubnets = append(dstSubnets, aws.ToString(sub.SubnetId))
			seenAZ[az] = true
		}
	}
	if len(dstSubnets) == 0 {
		return nil, nil, "", fmt.Errorf("no subnets found in destination VPC %s for AZs %v", dstVpcID, azList)
	}

	// Get default security group of the destination VPC.
	sgOut, sgerr := dstEC2.DescribeSecurityGroups(ctx, &ec2.DescribeSecurityGroupsInput{
		Filters: []ec2types.Filter{
			{Name: aws.String("vpc-id"), Values: []string{dstVpcID}},
			{Name: aws.String("group-name"), Values: []string{"default"}},
		},
	})
	var dstSGs []string
	if sgerr == nil && len(sgOut.SecurityGroups) > 0 {
		dstSGs = []string{aws.ToString(sgOut.SecurityGroups[0].GroupId)}
	}

	netMsg := fmt.Sprintf("matched by %s → vpc=%s subnets=%v sgs=%v", matchedBy, dstVpcID, dstSubnets, dstSGs)
	return dstSubnets, dstSGs, netMsg, nil
}

// arnAccountID extracts the AWS account ID from an ARN string.
// e.g. "arn:aws:ecs:us-east-1:123456789012:cluster/foo" → "123456789012"
func arnAccountID(arn string) string {
	parts := strings.Split(arn, ":")
	if len(parts) >= 5 {
		return parts[4]
	}
	return ""
}

// copyECSTaskDefinition reads a task definition from the source ECS client and
// registers it in the destination, replacing all occurrences of srcAccountID
// with dstAccountID (IAM role ARNs, ECR image URIs, etc.).
// Returns the "family:revision" string of the newly registered definition.
func copyECSTaskDefinition(ctx context.Context, srcCli, dstCli *ecs.Client, taskDefFamilyRevision, srcAccountID, dstAccountID string) (string, error) {
	out, err := srcCli.DescribeTaskDefinition(ctx, &ecs.DescribeTaskDefinitionInput{
		TaskDefinition: aws.String(taskDefFamilyRevision),
	})
	if err != nil {
		return "", fmt.Errorf("describe task definition: %w", err)
	}
	td := out.TaskDefinition

	replaceAccount := func(s string) string {
		if srcAccountID == "" || srcAccountID == dstAccountID {
			return s
		}
		return strings.ReplaceAll(s, srcAccountID, dstAccountID)
	}

	in := &ecs.RegisterTaskDefinitionInput{
		Family:                  td.Family,
		ContainerDefinitions:    td.ContainerDefinitions,
		Volumes:                 td.Volumes,
		PlacementConstraints:    td.PlacementConstraints,
		RequiresCompatibilities: td.RequiresCompatibilities,
		NetworkMode:             td.NetworkMode,
		Cpu:                     td.Cpu,
		Memory:                  td.Memory,
		IpcMode:                 td.IpcMode,
		PidMode:                 td.PidMode,
		RuntimePlatform:         td.RuntimePlatform,
	}
	if td.ExecutionRoleArn != nil {
		in.ExecutionRoleArn = aws.String(replaceAccount(*td.ExecutionRoleArn))
	}
	if td.TaskRoleArn != nil {
		in.TaskRoleArn = aws.String(replaceAccount(*td.TaskRoleArn))
	}
	// Replace account IDs in container image URIs (ECR).
	for i := range in.ContainerDefinitions {
		if in.ContainerDefinitions[i].Image != nil {
			in.ContainerDefinitions[i].Image = aws.String(replaceAccount(*in.ContainerDefinitions[i].Image))
		}
	}

	regOut, err := dstCli.RegisterTaskDefinition(ctx, in)
	if err != nil {
		return "", fmt.Errorf("register task definition: %w", err)
	}
	td2 := regOut.TaskDefinition
	return fmt.Sprintf("%s:%d", aws.ToString(td2.Family), td2.Revision), nil
}

// ── Secrets Manager helpers ────────────────────────────────────────────────

func createSecret(ctx context.Context, profile, region, name, value string) error {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(resolveProfileCase(profile)),
		config.WithRegion(region),
	)
	if err != nil {
		return err
	}
	cli := secretsmanager.NewFromConfig(cfg)
	_, err = cli.CreateSecret(ctx, &secretsmanager.CreateSecretInput{
		Name:         aws.String(name),
		SecretString: aws.String(value),
	})
	return err
}

func deleteSecret(ctx context.Context, profile, region, name string) error {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(resolveProfileCase(profile)),
		config.WithRegion(region),
	)
	if err != nil {
		return err
	}
	cli := secretsmanager.NewFromConfig(cfg)
	_, err = cli.DeleteSecret(ctx, &secretsmanager.DeleteSecretInput{
		SecretId:                   aws.String(name),
		ForceDeleteWithoutRecovery: aws.Bool(true),
	})
	return err
}

func preflightSecretsCopyCmd(src, dst panelState, names []string) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		action := fmt.Sprintf("check secrets copy conflicts %s/%s -> %s/%s", src.Profile, src.Region, dst.Profile, dst.Region)
		dstCfg, err := config.LoadDefaultConfig(ctx,
			config.WithSharedConfigProfile(resolveProfileCase(dst.Profile)),
			config.WithRegion(dst.Region),
		)
		if err != nil {
			return secretsCopyPreflightMsg{Src: src, Dst: dst, Err: err, Action: action}
		}
		dstCli := secretsmanager.NewFromConfig(dstCfg)
		existing := map[string]bool{}
		for _, name := range names {
			_, err := dstCli.DescribeSecret(ctx, &secretsmanager.DescribeSecretInput{SecretId: aws.String(name)})
			if err == nil {
				existing[name] = true
			}
		}
		return secretsCopyPreflightMsg{Src: src, Dst: dst, Names: names, Existing: existing, Action: action}
	}
}

func (m *model) startSecretsCopyJob(src, dst panelState, names []string, skipExisting bool, existing map[string]bool) tea.Cmd {
	srcCfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithSharedConfigProfile(resolveProfileCase(src.Profile)),
		config.WithRegion(src.Region),
	)
	if err != nil {
		m.status = "Failed to init source client: " + err.Error()
		return nil
	}
	dstCfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithSharedConfigProfile(resolveProfileCase(dst.Profile)),
		config.WithRegion(dst.Region),
	)
	if err != nil {
		m.status = "Failed to init destination client: " + err.Error()
		return nil
	}
	job := secretsCopyJobState{
		Src:          src,
		Dst:          dst,
		Names:        names,
		SkipExisting: skipExisting,
		Existing:     existing,
		SrcCli:       secretsmanager.NewFromConfig(srcCfg),
		DstCli:       secretsmanager.NewFromConfig(dstCfg),
	}
	m.secretsCopyJob = &job
	m.status = fmt.Sprintf("Copying secret 1/%d :: %s", len(names), names[0])
	m.pushLog(fmt.Sprintf("START secrets copy %s/%s -> %s/%s (%d)", src.Profile, src.Region, dst.Profile, dst.Region, len(names)))
	return secretsCopyStepCmd(job)
}

func secretsCopyStepCmd(job secretsCopyJobState) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		name := job.Names[job.Index]
		action := fmt.Sprintf("copy secret %s %s/%s -> %s/%s", name, job.Src.Profile, job.Src.Region, job.Dst.Profile, job.Dst.Region)

		if job.SkipExisting && job.Existing[name] {
			return secretsCopyProgressMsg{Job: job, SecretName: name, Skipped: true, Action: action}
		}

		valOut, err := job.SrcCli.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
			SecretId: aws.String(name),
		})
		if err != nil {
			return secretsCopyProgressMsg{Job: job, SecretName: name, Err: fmt.Errorf("get secret value: %w", err), Action: action}
		}

		if job.Existing[name] {
			_, err = job.DstCli.PutSecretValue(ctx, &secretsmanager.PutSecretValueInput{
				SecretId:     aws.String(name),
				SecretString: valOut.SecretString,
				SecretBinary: valOut.SecretBinary,
			})
		} else {
			_, err = job.DstCli.CreateSecret(ctx, &secretsmanager.CreateSecretInput{
				Name:         aws.String(name),
				SecretString: valOut.SecretString,
				SecretBinary: valOut.SecretBinary,
			})
		}
		if err != nil {
			return secretsCopyProgressMsg{Job: job, SecretName: name, Err: err, Action: action}
		}
		return secretsCopyProgressMsg{Job: job, SecretName: name, Action: action}
	}
}

// ─── RDS helpers ──────────────────────────────────────────────────────────────

func rdsCacheKey(profile, region, instanceID string) string {
	return profile + ":" + region + ":" + instanceID
}

func isPostgres(engine string) bool {
	e := strings.ToLower(engine)
	return e == "postgres" || e == "aurora-postgresql"
}

func sqlDriverName(engine string) string {
	if isPostgres(engine) {
		return "postgres"
	}
	return "mysql"
}

func rdsDSN(creds rdsCredentials, dbname string) string {
	if isPostgres(creds.Engine) {
		db := "postgres"
		if dbname != "" {
			db = dbname
		}
		return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=require&connect_timeout=10",
			url.PathEscape(creds.Username), url.PathEscape(creds.Password),
			creds.Endpoint, creds.Port, db)
	}
	// MySQL
	db := ""
	if dbname != "" {
		db = dbname
	}
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?timeout=10s&tls=skip-verify",
		creds.Username, creds.Password, creds.Endpoint, creds.Port, db)
}

func rdsListDatabases(ctx context.Context, conn *sql.DB, engine string) ([]resourceItem, error) {
	var rows *sql.Rows
	var err error
	if isPostgres(engine) {
		rows, err = conn.QueryContext(ctx, "SELECT datname FROM pg_database WHERE datistemplate=false ORDER BY datname")
	} else {
		rows, err = conn.QueryContext(ctx, "SHOW DATABASES")
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	skipMySQL := map[string]bool{"information_schema": true, "performance_schema": true, "mysql": true, "sys": true}
	items := make([]resourceItem, 0)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			continue
		}
		if !isPostgres(engine) && skipMySQL[name] {
			continue
		}
		items = append(items, resourceItem{Label: name, Value: name, Kind: "rds-db"})
	}
	return items, rows.Err()
}

func rdsListTables(ctx context.Context, conn *sql.DB, engine, dbname string) ([]resourceItem, error) {
	var rows *sql.Rows
	var err error
	if isPostgres(engine) {
		rows, err = conn.QueryContext(ctx, "SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename")
	} else {
		rows, err = conn.QueryContext(ctx, fmt.Sprintf("SHOW TABLES FROM `%s`", dbname))
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]resourceItem, 0)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			continue
		}
		items = append(items, resourceItem{Label: name, Value: name, Kind: "rds-table"})
	}
	return items, rows.Err()
}

func rdsSelectRows(ctx context.Context, conn *sql.DB, engine, dbname, tablename string, limit int) (string, error) {
	var query string
	if isPostgres(engine) {
		query = fmt.Sprintf(`SELECT * FROM "%s" LIMIT %d`, tablename, limit)
	} else {
		query = fmt.Sprintf("SELECT * FROM `%s`.`%s` LIMIT %d", dbname, tablename, limit)
	}
	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return "", err
	}
	var sb strings.Builder
	sb.WriteString(strings.Join(cols, " | "))
	sb.WriteString("\n")
	sb.WriteString(strings.Repeat("-", 80))
	sb.WriteString("\n")
	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	rowCount := 0
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			continue
		}
		parts := make([]string, len(cols))
		for i, v := range vals {
			if v == nil {
				parts[i] = "NULL"
			} else {
				parts[i] = fmt.Sprintf("%v", v)
			}
		}
		sb.WriteString(strings.Join(parts, " | "))
		sb.WriteString("\n")
		rowCount++
	}
	if rowCount == 0 {
		sb.WriteString("(no rows)\n")
	} else {
		sb.WriteString(fmt.Sprintf("\n(%d rows, LIMIT %d)", rowCount, limit))
	}
	return sb.String(), rows.Err()
}

func rdsGetInstanceInfoCmd(panelID panelID, p panelState, instanceID string) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		cfg, err := config.LoadDefaultConfig(ctx,
			config.WithSharedConfigProfile(resolveProfileCase(p.Profile)),
			config.WithRegion(p.Region),
		)
		if err != nil {
			return rdsInstanceInfoMsg{PanelID: panelID, InstanceID: instanceID, Err: err}
		}
		cli := awsrds.NewFromConfig(cfg)
		// Try as instance first.
		out, err := cli.DescribeDBInstances(ctx, &awsrds.DescribeDBInstancesInput{
			DBInstanceIdentifier: aws.String(instanceID),
		})
		if err == nil && len(out.DBInstances) > 0 {
			inst := out.DBInstances[0]
			endpoint := ""
			port := "3306"
			if inst.Endpoint != nil {
				endpoint = aws.ToString(inst.Endpoint.Address)
				if inst.Endpoint.Port != nil {
					port = strconv.Itoa(int(*inst.Endpoint.Port))
				}
			}
			return rdsInstanceInfoMsg{
				PanelID:    panelID,
				InstanceID: instanceID,
				Engine:     aws.ToString(inst.Engine),
				Endpoint:   endpoint,
				Port:       port,
			}
		}
		// Try as cluster.
		cout, cerr := cli.DescribeDBClusters(ctx, &awsrds.DescribeDBClustersInput{
			DBClusterIdentifier: aws.String(instanceID),
		})
		if cerr == nil && len(cout.DBClusters) > 0 {
			cl := cout.DBClusters[0]
			endpoint := aws.ToString(cl.Endpoint)
			port := "3306"
			if cl.Port != nil {
				port = strconv.Itoa(int(*cl.Port))
			}
			return rdsInstanceInfoMsg{
				PanelID:    panelID,
				InstanceID: instanceID,
				Engine:     aws.ToString(cl.Engine),
				Endpoint:   endpoint,
				Port:       port,
			}
		}
		if err != nil {
			return rdsInstanceInfoMsg{PanelID: panelID, InstanceID: instanceID, Err: err}
		}
		return rdsInstanceInfoMsg{PanelID: panelID, InstanceID: instanceID, Err: cerr}
	}
}

func rdsConnectCmd(pending *pendingRdsCredsState, password string) tea.Cmd {
	return func() tea.Msg {
		creds := rdsCredentials{
			Username: pending.Username,
			Password: password,
			Engine:   pending.Engine,
			Endpoint: pending.Endpoint,
			Port:     pending.Port,
		}
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		dsn := rdsDSN(creds, "")
		db, err := sql.Open(sqlDriverName(creds.Engine), dsn)
		if err != nil {
			return rdsConnectedMsg{PanelID: pending.PanelID, InstanceID: pending.InstanceID, Err: err}
		}
		db.SetMaxOpenConns(5)
		db.SetConnMaxLifetime(30 * time.Minute)
		if err := db.PingContext(ctx); err != nil {
			db.Close()
			return rdsConnectedMsg{PanelID: pending.PanelID, InstanceID: pending.InstanceID, Err: fmt.Errorf("ping failed: %w", err)}
		}
		cacheKey := rdsCacheKey(pending.Profile, pending.Region, pending.InstanceID)
		rdsConnCache.Lock()
		if old, ok := rdsConnCache.conns[cacheKey]; ok {
			old.Close()
		}
		rdsConnCache.conns[cacheKey] = db
		rdsConnCache.creds[cacheKey] = creds
		rdsConnCache.Unlock()
		// List databases.
		dbs, err := rdsListDatabases(ctx, db, creds.Engine)
		if err != nil {
			return rdsConnectedMsg{PanelID: pending.PanelID, InstanceID: pending.InstanceID, Err: fmt.Errorf("list databases: %w", err)}
		}
		items := []resourceItem{{Label: "← ..", Kind: "back", Value: ""}}
		items = append(items, dbs...)
		return rdsConnectedMsg{
			PanelID:    pending.PanelID,
			Profile:    pending.Profile,
			Region:     pending.Region,
			InstanceID: pending.InstanceID,
			Items:      items,
		}
	}
}

func rdsConnectToDBCmd(panelID panelID, p panelState, baseCreds rdsCredentials, dbname string) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		dsn := rdsDSN(baseCreds, dbname)
		db, err := sql.Open("postgres", dsn)
		if err != nil {
			return rdsDbConnectedMsg{PanelID: panelID, InstanceID: p.S3Bucket, DbName: dbname, Err: err}
		}
		db.SetMaxOpenConns(5)
		db.SetConnMaxLifetime(30 * time.Minute)
		if err := db.PingContext(ctx); err != nil {
			db.Close()
			return rdsDbConnectedMsg{PanelID: panelID, InstanceID: p.S3Bucket, DbName: dbname, Err: fmt.Errorf("ping failed: %w", err)}
		}
		cacheKey := rdsCacheKey(p.Profile, p.Region, p.S3Bucket)
		pgKey := cacheKey + ":" + dbname
		rdsConnCache.Lock()
		if old, ok := rdsConnCache.conns[pgKey]; ok {
			old.Close()
		}
		rdsConnCache.conns[pgKey] = db
		rdsConnCache.Unlock()
		tables, err := rdsListTables(ctx, db, baseCreds.Engine, dbname)
		if err != nil {
			return rdsDbConnectedMsg{PanelID: panelID, InstanceID: p.S3Bucket, DbName: dbname, Err: fmt.Errorf("list tables: %w", err)}
		}
		items := []resourceItem{{Label: "← ..", Kind: "back", Value: ""}}
		items = append(items, tables...)
		return rdsDbConnectedMsg{PanelID: panelID, InstanceID: p.S3Bucket, DbName: dbname, Items: items}
	}
}

func createRDSInstance(ctx context.Context, profile, region, identifier, engine, class, username, password string) error {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(resolveProfileCase(profile)),
		config.WithRegion(region),
	)
	if err != nil {
		return err
	}
	cli := awsrds.NewFromConfig(cfg)
	_, err = cli.CreateDBInstance(ctx, &awsrds.CreateDBInstanceInput{
		DBInstanceIdentifier: aws.String(identifier),
		DBInstanceClass:      aws.String(class),
		Engine:               aws.String(engine),
		MasterUsername:       aws.String(username),
		MasterUserPassword:   aws.String(password),
		AllocatedStorage:     aws.Int32(20),
		PubliclyAccessible:   aws.Bool(true),
	})
	return err
}

func deleteRDSInstance(ctx context.Context, profile, region, instanceID string) error {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(resolveProfileCase(profile)),
		config.WithRegion(region),
	)
	if err != nil {
		return err
	}
	cli := awsrds.NewFromConfig(cfg)
	_, err = cli.DeleteDBInstance(ctx, &awsrds.DeleteDBInstanceInput{
		DBInstanceIdentifier:   aws.String(instanceID),
		SkipFinalSnapshot:      aws.Bool(true),
		DeleteAutomatedBackups: aws.Bool(false),
	})
	return err
}

// rdsCopyWithDataCmd initialises a step-based RDS snapshot copy job and kicks
// off the first step. Progress is reported via rdsCopyProgressMsg / rdsCopyStepCmd.
func rdsCopyWithDataCmd(src, dst panelState, instanceID string) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		srcCfg, err := config.LoadDefaultConfig(ctx,
			config.WithSharedConfigProfile(resolveProfileCase(src.Profile)),
			config.WithRegion(src.Region),
		)
		if err != nil {
			return rdsCopyProgressMsg{Err: err, Job: rdsCopyJobState{InstanceID: instanceID}}
		}
		dstCfg, err := config.LoadDefaultConfig(ctx,
			config.WithSharedConfigProfile(resolveProfileCase(dst.Profile)),
			config.WithRegion(dst.Region),
		)
		if err != nil {
			return rdsCopyProgressMsg{Err: err, Job: rdsCopyJobState{InstanceID: instanceID}}
		}
		stsCli := sts.NewFromConfig(dstCfg)
		identity, err := stsCli.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
		if err != nil {
			return rdsCopyProgressMsg{Err: fmt.Errorf("get dst account: %w", err), Job: rdsCopyJobState{InstanceID: instanceID}}
		}
		now := time.Now()
		job := rdsCopyJobState{
			Src:          src,
			Dst:          dst,
			InstanceID:   instanceID,
			Step:         "init",
			StartedAt:    now,
			StepStartAt:  now,
			DstAccountID: aws.ToString(identity.Account),
			SrcCli:       awsrds.NewFromConfig(srcCfg),
			DstCli:       awsrds.NewFromConfig(dstCfg),
		}
		return rdsCopyProgressMsg{Job: job, Status: "Initialising RDS snapshot copy..."}
	}
}

// rdsCopyStepCmd executes one step of the copy pipeline and returns the next
// rdsCopyProgressMsg, which Update() uses to dispatch the following step.
func rdsCopyStepCmd(job rdsCopyJobState) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		switch job.Step {

		case "init":
			// Create source snapshot.
			job.SnapshotID = job.InstanceID + "-nc-aws-" + strconv.FormatInt(job.StartedAt.Unix(), 10)
			job.NewInstanceID = job.InstanceID + "-copy"
			_, err := job.SrcCli.CreateDBSnapshot(ctx, &awsrds.CreateDBSnapshotInput{
				DBInstanceIdentifier: aws.String(job.InstanceID),
				DBSnapshotIdentifier: aws.String(job.SnapshotID),
			})
			if err != nil {
				return rdsCopyProgressMsg{Job: job, Err: fmt.Errorf("create snapshot: %w", err)}
			}
			job.Step = "wait_src"
			job.StepStartAt = time.Now()
			job.PollCount = 0
			return rdsCopyProgressMsg{Job: job, Status: "Snapshot creation started..."}

		case "wait_src":
			if job.PollCount > 0 {
				select {
				case <-time.After(30 * time.Second):
				case <-ctx.Done():
					return rdsCopyProgressMsg{Job: job, Err: fmt.Errorf("timeout waiting for source snapshot")}
				}
			}
			desc, err := job.SrcCli.DescribeDBSnapshots(ctx, &awsrds.DescribeDBSnapshotsInput{
				DBSnapshotIdentifier: aws.String(job.SnapshotID),
			})
			job.PollCount++
			if err != nil {
				return rdsCopyProgressMsg{Job: job, Err: fmt.Errorf("describe snapshot: %w", err)}
			}
			if len(desc.DBSnapshots) == 0 || aws.ToString(desc.DBSnapshots[0].Status) != "available" {
				return rdsCopyProgressMsg{Job: job}
			}
			snapArn := aws.ToString(desc.DBSnapshots[0].DBSnapshotArn)
			kmsKeyID := aws.ToString(desc.DBSnapshots[0].KmsKeyId)

			if kmsKeyID != "" {
				// Check if the snapshot uses the AWS-managed key, which cannot be shared
				// cross-account. If so, re-encrypt within the source account using a CMK first.
				managed, err := isAWSManagedKey(ctx, job.Src.Profile, job.Src.Region, kmsKeyID)
				if err != nil {
					return rdsCopyProgressMsg{Job: job, Err: fmt.Errorf("check KMS key type: %w", err)}
				}
				if managed {
					// Find or create the nc-aws CMK in the source account.
					cmkARN, err := findOrCreateNcAwsCMK(ctx, job.Src.Profile, job.Src.Region)
					if err != nil {
						return rdsCopyProgressMsg{Job: job, Err: fmt.Errorf("create CMK: %w", err)}
					}
					job.KmsKeyID = cmkARN
					job.ReencryptSnapID = job.SnapshotID + "-cmk"
					_, err = job.SrcCli.CopyDBSnapshot(ctx, &awsrds.CopyDBSnapshotInput{
						SourceDBSnapshotIdentifier: aws.String(snapArn),
						TargetDBSnapshotIdentifier: aws.String(job.ReencryptSnapID),
						KmsKeyId:                   aws.String(cmkARN),
					})
					if err != nil {
						return rdsCopyProgressMsg{Job: job, Err: fmt.Errorf("re-encrypt snapshot: %w", err)}
					}
					job.Step = "wait_reencrypt"
					job.StepStartAt = time.Now()
					job.PollCount = 0
					return rdsCopyProgressMsg{Job: job, Status: "aws/rds key detected — re-encrypting snapshot with CMK..."}
				}
				// Customer-managed key: share it with the destination account.
				if err := shareKmsKeyWithAccount(ctx, job.Src.Profile, job.Src.Region, kmsKeyID, job.DstAccountID); err != nil {
					return rdsCopyProgressMsg{Job: job, Err: fmt.Errorf("share KMS key: %w", err)}
				}
				job.KmsKeyID = kmsKeyID
			}
			return rdsShareAndCopyToDst(ctx, job)

		case "wait_reencrypt":
			// Wait for the CMK-encrypted copy of the source snapshot to become available.
			if job.PollCount > 0 {
				select {
				case <-time.After(30 * time.Second):
				case <-ctx.Done():
					return rdsCopyProgressMsg{Job: job, Err: fmt.Errorf("timeout waiting for re-encrypted snapshot")}
				}
			}
			desc, err := job.SrcCli.DescribeDBSnapshots(ctx, &awsrds.DescribeDBSnapshotsInput{
				DBSnapshotIdentifier: aws.String(job.ReencryptSnapID),
			})
			job.PollCount++
			if err != nil {
				return rdsCopyProgressMsg{Job: job, Err: fmt.Errorf("describe re-encrypted snapshot: %w", err)}
			}
			if len(desc.DBSnapshots) == 0 || aws.ToString(desc.DBSnapshots[0].Status) != "available" {
				return rdsCopyProgressMsg{Job: job}
			}
			// Re-encryption done: share the CMK and proceed.
			if err := shareKmsKeyWithAccount(ctx, job.Src.Profile, job.Src.Region, job.KmsKeyID, job.DstAccountID); err != nil {
				return rdsCopyProgressMsg{Job: job, Err: fmt.Errorf("share CMK: %w", err)}
			}
			// Point SnapshotID at the re-encrypted version for the share + copy steps.
			job.SnapshotID = job.ReencryptSnapID
			return rdsShareAndCopyToDst(ctx, job)

		case "wait_dst":
			if job.PollCount > 0 {
				select {
				case <-time.After(30 * time.Second):
				case <-ctx.Done():
					return rdsCopyProgressMsg{Job: job, Err: fmt.Errorf("timeout waiting for dst snapshot")}
				}
			}
			desc, err := job.DstCli.DescribeDBSnapshots(ctx, &awsrds.DescribeDBSnapshotsInput{
				DBSnapshotIdentifier: aws.String(job.CopiedSnapID),
			})
			job.PollCount++
			if err != nil {
				return rdsCopyProgressMsg{Job: job, Err: fmt.Errorf("describe dst snapshot: %w", err)}
			}
			if len(desc.DBSnapshots) == 0 || aws.ToString(desc.DBSnapshots[0].Status) != "available" {
				return rdsCopyProgressMsg{Job: job}
			}
			job.DstSnapArn = aws.ToString(desc.DBSnapshots[0].DBSnapshotArn)
			job.Step = "restore"
			job.StepStartAt = time.Now()
			return rdsCopyProgressMsg{Job: job, Status: "Destination snapshot ready — initiating restore..."}

		case "restore":
			_, err := job.DstCli.RestoreDBInstanceFromDBSnapshot(ctx, &awsrds.RestoreDBInstanceFromDBSnapshotInput{
				DBInstanceIdentifier: aws.String(job.NewInstanceID),
				DBSnapshotIdentifier: aws.String(job.DstSnapArn),
				PubliclyAccessible:   aws.Bool(true),
			})
			if err != nil {
				return rdsCopyProgressMsg{Job: job, Err: fmt.Errorf("restore: %w", err)}
			}
			total := fmtDuration(time.Since(job.StartedAt))
			return rdsCopyProgressMsg{
				Job:  job,
				Done: true,
				Status: fmt.Sprintf("Restore initiated as %s in %s/%s (total: %s)",
					job.NewInstanceID, job.Dst.Profile, job.Dst.Region, total),
			}
		}
		return rdsCopyProgressMsg{Job: job, Err: fmt.Errorf("unknown step: %s", job.Step)}
	}
}

func fmtDuration(d time.Duration) string {
	d = d.Round(time.Second)
	m := int(d.Minutes())
	s := int(d.Seconds()) % 60
	if m == 0 {
		return fmt.Sprintf("%ds", s)
	}
	if s == 0 {
		return fmt.Sprintf("%dm", m)
	}
	return fmt.Sprintf("%dm%ds", m, s)
}

func rdsStepLabel(step string, poll int) string {
	switch step {
	case "init":
		return "creating snapshot"
	case "wait_src":
		return fmt.Sprintf("waiting for source snapshot [poll #%d]", poll)
	case "wait_reencrypt":
		return fmt.Sprintf("re-encrypting with CMK (aws/rds→CMK) [poll #%d]", poll)
	case "wait_dst":
		return fmt.Sprintf("copying to destination account [poll #%d]", poll)
	case "restore":
		return "initiating restore"
	default:
		return step
	}
}

func rdsETAStr(job rdsCopyJobState) string {
	// Rough estimates per step.
	estimates := map[string]time.Duration{
		"wait_src":       7 * time.Minute,
		"wait_reencrypt": 5 * time.Minute,
		"wait_dst":       6 * time.Minute,
	}
	est, ok := estimates[job.Step]
	if !ok {
		return ""
	}
	remaining := est - time.Since(job.StepStartAt)
	if remaining <= 0 {
		return " (taking longer than expected)"
	}
	return fmt.Sprintf(" (~%s remaining)", fmtDuration(remaining))
}

func rdsCopySchemaOnlyCmd(src, dst panelState, instanceID string) tea.Cmd {
	return func() tea.Msg {
		action := fmt.Sprintf("rds copy schema only %s -> %s/%s", instanceID, dst.Profile, dst.Region)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		srcCfg, err := config.LoadDefaultConfig(ctx,
			config.WithSharedConfigProfile(resolveProfileCase(src.Profile)),
			config.WithRegion(src.Region),
		)
		if err != nil {
			return opMsg{Err: err, Action: action}
		}
		srcCli := awsrds.NewFromConfig(srcCfg)
		out, err := srcCli.DescribeDBInstances(ctx, &awsrds.DescribeDBInstancesInput{
			DBInstanceIdentifier: aws.String(instanceID),
		})
		if err != nil || len(out.DBInstances) == 0 {
			return opMsg{Err: fmt.Errorf("describe source instance: %w", err), Action: action}
		}
		src_ := out.DBInstances[0]

		dstCfg, err := config.LoadDefaultConfig(ctx,
			config.WithSharedConfigProfile(resolveProfileCase(dst.Profile)),
			config.WithRegion(dst.Region),
		)
		if err != nil {
			return opMsg{Err: err, Action: action}
		}
		dstCli := awsrds.NewFromConfig(dstCfg)
		newID := instanceID + "-copy"
		_, err = dstCli.CreateDBInstance(ctx, &awsrds.CreateDBInstanceInput{
			DBInstanceIdentifier: aws.String(newID),
			DBInstanceClass:      src_.DBInstanceClass,
			Engine:               src_.Engine,
			MasterUsername:       src_.MasterUsername,
			MasterUserPassword:   aws.String("ChangeMe123!"),
			AllocatedStorage:     src_.AllocatedStorage,
			PubliclyAccessible:   aws.Bool(true),
		})
		if err != nil {
			return opMsg{Err: fmt.Errorf("create destination instance: %w", err), Action: action}
		}
		return opMsg{Status: fmt.Sprintf("Instance creation started as %s in %s/%s (change password immediately)", newID, dst.Profile, dst.Region), Action: action}
	}
}

// selectedRdsValues returns the Values of all rds-instance or rds-cluster items
// that are marked, or the focused item if nothing is marked.
func selectedRdsValues(p panelState) []string {
	var vals []string
	for _, r := range p.Resources {
		if (r.Kind == "rds-instance" || r.Kind == "rds-cluster") && p.Marked[r.Value] {
			vals = append(vals, r.Value)
		}
	}
	if len(vals) == 0 {
		it, ok := selectedItem(p)
		if ok && (it.Kind == "rds-instance" || it.Kind == "rds-cluster") {
			vals = []string{it.Value}
		}
	}
	return vals
}

func renderRdsCopyConfirm(m model) string {
	th := currentTheme(m)
	dialogStyle := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color(th.DialogBorder)).
		Padding(1, 3)
	src := m.rdsCopyConf.Src
	dst := m.rdsCopyConf.Dst
	body := fmt.Sprintf(
		"Copy RDS instance: %s\n\nFrom: %s / %s\nTo:   %s / %s\n\n[w]  with data (snapshot restore)\n[s]  schema only (empty instance)\n[esc] cancel",
		m.rdsCopyConf.InstanceID,
		src.Profile, src.Region,
		dst.Profile, dst.Region,
	)
	dialog := dialogStyle.Render(body)
	return lipgloss.Place(m.width, m.height, lipgloss.Center, lipgloss.Center, dialog)
}

// isAWSManagedKey returns true if the given KMS key ID belongs to an AWS-managed key.
func isAWSManagedKey(ctx context.Context, profile, region, keyID string) (bool, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(resolveProfileCase(profile)),
		config.WithRegion(region),
	)
	if err != nil {
		return false, err
	}
	kmsCli := kms.NewFromConfig(cfg)
	descOut, err := kmsCli.DescribeKey(ctx, &kms.DescribeKeyInput{KeyId: aws.String(keyID)})
	if err != nil {
		return false, fmt.Errorf("describe KMS key: %w", err)
	}
	if descOut.KeyMetadata == nil {
		return false, nil
	}
	return descOut.KeyMetadata.KeyManager == kmstypes.KeyManagerTypeAws, nil
}

// findOrCreateNcAwsCMK returns the ARN of the nc-aws cross-account CMK in the given
// account/region, creating it (with alias/nc-aws-rds-copy) if it does not exist yet.
func findOrCreateNcAwsCMK(ctx context.Context, profile, region string) (string, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(resolveProfileCase(profile)),
		config.WithRegion(region),
	)
	if err != nil {
		return "", err
	}
	kmsCli := kms.NewFromConfig(cfg)
	const aliasName = "alias/nc-aws-rds-copy"
	// Try to find an existing key with this alias.
	descOut, err := kmsCli.DescribeKey(ctx, &kms.DescribeKeyInput{KeyId: aws.String(aliasName)})
	if err == nil && descOut.KeyMetadata != nil {
		return aws.ToString(descOut.KeyMetadata.Arn), nil
	}
	// Create a new symmetric CMK.
	createOut, err := kmsCli.CreateKey(ctx, &kms.CreateKeyInput{
		Description: aws.String("nc-aws cross-account RDS snapshot copy key"),
		KeyUsage:    kmstypes.KeyUsageTypeEncryptDecrypt,
	})
	if err != nil {
		return "", fmt.Errorf("create KMS key: %w", err)
	}
	keyARN := aws.ToString(createOut.KeyMetadata.Arn)
	// Attach the alias so future operations reuse the same key.
	_, _ = kmsCli.CreateAlias(ctx, &kms.CreateAliasInput{
		AliasName:   aws.String(aliasName),
		TargetKeyId: aws.String(keyARN),
	})
	return keyARN, nil
}

// rdsShareAndCopyToDst shares job.SnapshotID with the destination account and starts
// the CopyDBSnapshot call there. Advances job to step "wait_dst".
func rdsShareAndCopyToDst(ctx context.Context, job rdsCopyJobState) rdsCopyProgressMsg {
	// Share snapshot attribute with destination account.
	_, err := job.SrcCli.ModifyDBSnapshotAttribute(ctx, &awsrds.ModifyDBSnapshotAttributeInput{
		DBSnapshotIdentifier: aws.String(job.SnapshotID),
		AttributeName:        aws.String("restore"),
		ValuesToAdd:          []string{job.DstAccountID},
	})
	if err != nil {
		return rdsCopyProgressMsg{Job: job, Err: fmt.Errorf("share snapshot: %w", err)}
	}
	// Get ARN of the snapshot we're sharing.
	descOut, err := job.SrcCli.DescribeDBSnapshots(ctx, &awsrds.DescribeDBSnapshotsInput{
		DBSnapshotIdentifier: aws.String(job.SnapshotID),
	})
	if err != nil || len(descOut.DBSnapshots) == 0 {
		return rdsCopyProgressMsg{Job: job, Err: fmt.Errorf("describe snapshot for ARN: %w", err)}
	}
	snapArn := aws.ToString(descOut.DBSnapshots[0].DBSnapshotArn)
	// Start copy into destination account (re-encrypts with destination's aws/rds key).
	job.CopiedSnapID = job.SnapshotID + "-dst"
	copyInput := &awsrds.CopyDBSnapshotInput{
		SourceDBSnapshotIdentifier: aws.String(snapArn),
		TargetDBSnapshotIdentifier: aws.String(job.CopiedSnapID),
		KmsKeyId:                   aws.String("alias/aws/rds"),
	}
	if job.Src.Region != job.Dst.Region {
		copyInput.SourceRegion = aws.String(job.Src.Region)
	}
	_, err = job.DstCli.CopyDBSnapshot(ctx, copyInput)
	if err != nil {
		return rdsCopyProgressMsg{Job: job, Err: fmt.Errorf("copy to dst account: %w", err)}
	}
	job.Step = "wait_dst"
	job.StepStartAt = time.Now()
	job.PollCount = 0
	return rdsCopyProgressMsg{Job: job, Status: "Snapshot shared — copying to destination account..."}
}

// shareKmsKeyWithAccount adds the destination account to the key policy of the given CMK.
// Caller must ensure the key is customer-managed (not AWS-managed).
func shareKmsKeyWithAccount(ctx context.Context, profile, region, keyID, dstAccountID string) error {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(resolveProfileCase(profile)),
		config.WithRegion(region),
	)
	if err != nil {
		return err
	}
	kmsCli := kms.NewFromConfig(cfg)
	policyOut, err := kmsCli.GetKeyPolicy(ctx, &kms.GetKeyPolicyInput{
		KeyId:      aws.String(keyID),
		PolicyName: aws.String("default"),
	})
	if err != nil {
		return fmt.Errorf("get key policy: %w", err)
	}
	var policy map[string]any
	if err := json.Unmarshal([]byte(aws.ToString(policyOut.Policy)), &policy); err != nil {
		return fmt.Errorf("parse key policy: %w", err)
	}
	stmts, _ := policy["Statement"].([]any)
	sidWanted := "nc-aws-xacct-" + dstAccountID
	for _, s := range stmts {
		if sm, ok := s.(map[string]any); ok && sm["Sid"] == sidWanted {
			return nil // already present
		}
	}
	policy["Statement"] = append(stmts, map[string]any{
		"Sid":    sidWanted,
		"Effect": "Allow",
		"Principal": map[string]any{
			"AWS": "arn:aws:iam::" + dstAccountID + ":root",
		},
		"Action":   []string{"kms:Decrypt", "kms:CreateGrant", "kms:DescribeKey", "kms:ReEncrypt*", "kms:GenerateDataKey*"},
		"Resource": "*",
	})
	updated, err := json.Marshal(policy)
	if err != nil {
		return fmt.Errorf("marshal key policy: %w", err)
	}
	_, err = kmsCli.PutKeyPolicy(ctx, &kms.PutKeyPolicyInput{
		KeyId:      aws.String(keyID),
		PolicyName: aws.String("default"),
		Policy:     aws.String(string(updated)),
	})
	return err
}
