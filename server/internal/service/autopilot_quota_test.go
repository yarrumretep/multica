package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/multica-ai/multica/server/internal/entitlement"
	"github.com/multica-ai/multica/server/internal/entitlement/entitlementtest"
	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

type autopilotQuotaFixture struct {
	pool          *pgxpool.Pool
	queries       *db.Queries
	service       *AutopilotService
	stub          *entitlementtest.Stub
	workspace     uuid.UUID
	workspaceID   pgtype.UUID
	autopilotID   pgtype.UUID
	agentID       pgtype.UUID
	publisherID   pgtype.UUID
	issueID       pgtype.UUID
	periodStart   time.Time
	periodEnd     time.Time
	resetAt       time.Time
	policyLimit   int
	createRunArgs db.CreateAutopilotRunParams
}

type autopilotQuotaMetricsRecorder struct {
	mu        sync.Mutex
	decisions map[string]int
}

type countingTxStarter struct {
	inner  TxStarter
	begins atomic.Int64
}

func (s *countingTxStarter) Begin(ctx context.Context) (pgx.Tx, error) {
	s.begins.Add(1)
	return s.inner.Begin(ctx)
}

func (m *autopilotQuotaMetricsRecorder) RecordAutopilotQuotaDecision(action, source, result string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.decisions == nil {
		m.decisions = make(map[string]int)
	}
	m.decisions[action+"\x00"+source+"\x00"+result]++
}

func (m *autopilotQuotaMetricsRecorder) count(action, source, result string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.decisions[action+"\x00"+source+"\x00"+result]
}

func newAutopilotQuotaFixture(t *testing.T, action entitlement.Action, limit int) *autopilotQuotaFixture {
	t.Helper()
	pool := newResolveOriginatorPool(t)
	q := db.New(pool)
	workspaceIDString, publisherID, agentID, issueIDString := seedAttributionFixture(t, pool)
	autopilotIDString, _ := seedRunOnlyAutopilot(t, pool, workspaceIDString, agentID, publisherID)
	workspaceID := util.MustParseUUID(workspaceIDString)
	workspace := uuid.UUID(workspaceID.Bytes)
	periodStart := time.Now().UTC().Truncate(time.Second)
	periodEnd := periodStart.Add(37 * time.Hour)
	stub := entitlementtest.New()
	fixture := &autopilotQuotaFixture{
		pool: pool, queries: q, stub: stub, workspace: workspace,
		workspaceID: workspaceID, autopilotID: util.MustParseUUID(autopilotIDString),
		agentID: util.MustParseUUID(agentID), publisherID: util.MustParseUUID(publisherID),
		issueID:     util.MustParseUUID(issueIDString),
		periodStart: periodStart, periodEnd: periodEnd, resetAt: periodEnd,
		policyLimit: limit,
		createRunArgs: db.CreateAutopilotRunParams{
			AutopilotID: util.MustParseUUID(autopilotIDString), Source: "api", Status: "running",
		},
	}
	fixture.service = &AutopilotService{
		Queries: q, TxStarter: pool, Bus: events.New(), Entitlements: stub,
	}
	fixture.setPolicy(action, periodStart, periodEnd, limit)
	t.Cleanup(func() {
		pool.Exec(context.Background(), `DELETE FROM autopilot_quota_reservation WHERE workspace_id = $1`, workspaceID)
		pool.Exec(context.Background(), `DELETE FROM autopilot_quota_period WHERE workspace_id = $1`, workspaceID)
	})
	return fixture
}

func (f *autopilotQuotaFixture) setPolicy(action entitlement.Action, start, end time.Time, limit int) {
	f.stub.Set(f.workspace, entitlement.GateAutopilotRuns, entitlement.Decision{
		Gate: entitlement.Gate{
			Action: action, Limit: &limit,
			PeriodStart: &start, PeriodEnd: &end, ResetAt: &end,
		},
		PolicyRevision: 7, SubscriptionVersion: 11,
	})
}

func (f *autopilotQuotaFixture) setNotificationPolicy(limit int, thresholds ...entitlement.NotificationThreshold) {
	f.stub.Set(f.workspace, entitlement.GateAutopilotRuns, entitlement.Decision{
		Gate: entitlement.Gate{
			Action: entitlement.ActionEnforce, Limit: &limit,
			PeriodStart: &f.periodStart, PeriodEnd: &f.periodEnd, ResetAt: &f.resetAt,
			Notifications: &entitlement.NotificationPolicy{
				Thresholds: thresholds, OnRejection: entitlement.NotificationEveryAttempt,
				AutomatedRejectionMinInterval: 24 * time.Hour,
			},
		},
		PolicyRevision: 8, SubscriptionVersion: 11,
	})
}

func addAutopilotQuotaMember(t *testing.T, f *autopilotQuotaFixture, role string) pgtype.UUID {
	t.Helper()
	ctx := context.Background()
	var userID pgtype.UUID
	email := fmt.Sprintf("quota-notice-%s-%d@multica.test", role, time.Now().UnixNano())
	if err := f.pool.QueryRow(ctx, `INSERT INTO "user" (name, email) VALUES ('Quota Notice Member', $1) RETURNING id`, email).Scan(&userID); err != nil {
		t.Fatalf("insert quota notice user: %v", err)
	}
	if _, err := f.pool.Exec(ctx, `INSERT INTO member (workspace_id, user_id, role) VALUES ($1, $2, $3)`, f.workspaceID, userID, role); err != nil {
		t.Fatalf("insert quota notice member: %v", err)
	}
	t.Cleanup(func() {
		f.pool.Exec(context.Background(), `DELETE FROM member WHERE workspace_id = $1 AND user_id = $2`, f.workspaceID, userID)
		f.pool.Exec(context.Background(), `DELETE FROM "user" WHERE id = $1`, userID)
	})
	return userID
}

func TestAutopilotQuotaDisabledDoesNotReadQuotaTables(t *testing.T) {
	ctx := context.Background()
	workspace := uuid.New()
	workspaceID := pgtype.UUID{Bytes: workspace, Valid: true}

	t.Run("off", func(t *testing.T) {
		stub := entitlementtest.New()
		stub.Set(workspace, entitlement.GateAutopilotRuns, entitlement.Decision{
			Gate: entitlement.Gate{Action: entitlement.ActionOff},
		})
		svc := &AutopilotService{Entitlements: stub} // Queries intentionally nil
		usage, err := svc.AutopilotQuotaUsage(ctx, workspaceID)
		if err != nil || usage.Enabled {
			t.Fatalf("off usage = %+v, %v; want disabled", usage, err)
		}
	})

	t.Run("malformed", func(t *testing.T) {
		stub := entitlementtest.New()
		limit := 2
		stub.Set(workspace, entitlement.GateAutopilotRuns, entitlement.Decision{
			Gate: entitlement.Gate{Action: entitlement.ActionEnforce, Limit: &limit},
		})
		svc := &AutopilotService{Entitlements: stub} // missing interval; Queries intentionally nil
		usage, err := svc.AutopilotQuotaUsage(ctx, workspaceID)
		if err != nil || usage.Enabled {
			t.Fatalf("malformed usage = %+v, %v; want fail-open disabled", usage, err)
		}
	})
}

func TestAutopilotQuotaRejectsUnknownExecutionSource(t *testing.T) {
	fixture := newAutopilotQuotaFixture(t, entitlement.ActionEnforce, 1)
	_, _, err := fixture.service.createAutopilotRunWithQuota(
		context.Background(), fixture.workspaceID, "manul", "invalid-source", fixture.createRunArgs,
	)
	if err == nil || !strings.Contains(err.Error(), "invalid autopilot execution source") {
		t.Fatalf("unknown source error = %v, want application validation", err)
	}
}

func TestAutopilotQuotaEnforcesBoundaryAndFinalizesIdempotently(t *testing.T) {
	pool := newResolveOriginatorPool(t)
	ctx := context.Background()
	q := db.New(pool)
	workspaceIDString, publisherID, agentID, _ := seedAttributionFixture(t, pool)
	autopilotIDString, _ := seedRunOnlyAutopilot(t, pool, workspaceIDString, agentID, publisherID)
	workspaceID := util.MustParseUUID(workspaceIDString)
	autopilotID := util.MustParseUUID(autopilotIDString)
	t.Cleanup(func() {
		pool.Exec(context.Background(), `DELETE FROM autopilot_quota_reservation WHERE workspace_id = $1`, workspaceID)
		pool.Exec(context.Background(), `DELETE FROM autopilot_quota_period WHERE workspace_id = $1`, workspaceID)
	})

	periodStart := time.Now().UTC().Truncate(time.Second)
	periodEnd := periodStart.Add(37 * time.Hour) // opaque Cloud interval, deliberately not a calendar month
	resetAt := periodEnd
	limit := 2
	stub := entitlementtest.New()
	stub.Set(uuid.UUID(workspaceID.Bytes), entitlement.GateAutopilotRuns, entitlement.Decision{
		Gate: entitlement.Gate{
			Action: entitlement.ActionEnforce, Limit: &limit,
			PeriodStart: &periodStart, PeriodEnd: &periodEnd, ResetAt: &resetAt,
		},
		PolicyRevision: 7, SubscriptionVersion: 11,
	})
	svc := &AutopilotService{Queries: q, TxStarter: pool, Bus: events.New(), Entitlements: stub}
	params := db.CreateAutopilotRunParams{
		AutopilotID: autopilotID, Source: "api", Status: "running",
	}

	runs := make([]db.AutopilotRun, 0, limit)
	for i, key := range []string{"boundary-1", "boundary-2"} {
		run, _, err := svc.createAutopilotRunWithQuota(ctx, workspaceID, "api", key, params)
		if err != nil {
			t.Fatalf("admission %d: %v", i+1, err)
		}
		runs = append(runs, run)
	}
	reused, wasReused, err := svc.createAutopilotRunWithQuota(ctx, workspaceID, "api", "boundary-1", params)
	if err != nil || !wasReused || reused.ID.Bytes != runs[0].ID.Bytes {
		t.Fatalf("idempotent reuse = %s, %v; want %s", util.UUIDToString(reused.ID), err, util.UUIDToString(runs[0].ID))
	}
	_, _, err = svc.createAutopilotRunWithQuota(ctx, workspaceID, "api", "boundary-3", params)
	var quotaErr *AutopilotQuotaExceededError
	if !errors.As(err, &quotaErr) {
		t.Fatalf("N+1 admission error = %v, want quota exceeded", err)
	}

	usage, err := svc.AutopilotQuotaUsage(ctx, workspaceID)
	if err != nil || usage.Used == nil || usage.Reserved == nil || *usage.Used != 0 || *usage.Reserved != int64(limit) {
		t.Fatalf("reserved usage = %+v, %v", usage, err)
	}
	if got := usage.BlockedCounts["api"]; got != 1 {
		t.Fatalf("blocked API count = %d, want 1", got)
	}
	if _, err := settleAutopilotQuota(ctx, q, runs[0].QuotaReservationID, true); err != nil {
		t.Fatalf("consume: %v", err)
	}
	if _, err := settleAutopilotQuota(ctx, q, runs[0].QuotaReservationID, true); err != nil {
		t.Fatalf("duplicate consume: %v", err)
	}
	if _, err := settleAutopilotQuota(ctx, q, runs[1].QuotaReservationID, false); err != nil {
		t.Fatalf("release: %v", err)
	}
	// Consumed usage is monotonic: later issue cancellation/deletion cannot
	// turn a successfully created issue back into quota.
	if _, err := settleAutopilotQuota(ctx, q, runs[0].QuotaReservationID, false); err != nil {
		t.Fatalf("release consumed reservation: %v", err)
	}
	usage, err = svc.AutopilotQuotaUsage(ctx, workspaceID)
	if err != nil || *usage.Used != 1 || *usage.Reserved != 0 {
		t.Fatalf("final usage = %+v, %v; want one immutable consumed unit", usage, err)
	}
}

func TestAutopilotQuotaThresholdNoticesReachEveryMemberOnce(t *testing.T) {
	fixture := newAutopilotQuotaFixture(t, entitlement.ActionEnforce, 10)
	fixture.setNotificationPolicy(10,
		entitlement.NotificationThreshold{Key: "usage_50", Percent: 50, AtCount: 5},
		entitlement.NotificationThreshold{Key: "usage_80", Percent: 80, AtCount: 8},
		entitlement.NotificationThreshold{Key: "usage_90", Percent: 90, AtCount: 9},
	)
	addAutopilotQuotaMember(t, fixture, "admin")
	addAutopilotQuotaMember(t, fixture, "member")
	ctx := context.Background()

	for i := 1; i <= 9; i++ {
		if _, _, err := fixture.service.createAutopilotRunWithQuota(
			ctx, fixture.workspaceID, "api", fmt.Sprintf("threshold-%d", i), fixture.createRunArgs,
		); err != nil {
			t.Fatalf("admission %d: %v", i, err)
		}
	}

	var count int
	if err := fixture.pool.QueryRow(ctx, `
		SELECT count(*) FROM inbox_item
		WHERE workspace_id = $1 AND type = 'autopilot_quota_warning'`, fixture.workspaceID).Scan(&count); err != nil {
		t.Fatalf("count threshold inbox rows: %v", err)
	}
	if count != 9 {
		t.Fatalf("threshold inbox rows = %d, want 3 thresholds x 3 members", count)
	}

	period, err := fixture.queries.GetAutopilotQuotaPeriod(ctx, db.GetAutopilotQuotaPeriodParams{
		WorkspaceID: fixture.workspaceID,
		PeriodStart: pgtype.Timestamptz{Time: fixture.periodStart, Valid: true},
		PeriodEnd:   pgtype.Timestamptz{Time: fixture.periodEnd, Valid: true},
	})
	if err != nil {
		t.Fatalf("load quota period: %v", err)
	}
	var notified map[string]bool
	if err := json.Unmarshal(period.NotifiedThresholds, &notified); err != nil {
		t.Fatalf("decode notified thresholds: %v", err)
	}
	for _, key := range []string{"usage_50", "usage_80", "usage_90"} {
		if !notified[key] {
			t.Fatalf("threshold %q was not persisted: %#v", key, notified)
		}
	}
}

func TestAutopilotQuotaThresholdNoticeFastPathAvoidsWriteTransaction(t *testing.T) {
	tests := []struct {
		name       string
		limit      int
		thresholds []entitlement.NotificationThreshold
		runs       int
		wantBegins int64
	}{
		{name: "rejection only", limit: 1, runs: 1, wantBegins: 1},
		{
			name: "threshold not reached", limit: 10, runs: 1,
			thresholds: []entitlement.NotificationThreshold{{Key: "usage_50", Percent: 50, AtCount: 5}},
			wantBegins: 1,
		},
		{
			name: "threshold already delivered", limit: 3, runs: 2,
			thresholds: []entitlement.NotificationThreshold{{Key: "usage_50", Percent: 50, AtCount: 1}},
			// Two admission transactions plus the first threshold delivery. The
			// second admission sees the persisted marker without taking the lock.
			wantBegins: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fixture := newAutopilotQuotaFixture(t, entitlement.ActionEnforce, tt.limit)
			fixture.setNotificationPolicy(tt.limit, tt.thresholds...)
			starter := &countingTxStarter{inner: fixture.pool}
			fixture.service.TxStarter = starter
			for i := 0; i < tt.runs; i++ {
				if _, _, err := fixture.service.createAutopilotRunWithQuota(
					context.Background(), fixture.workspaceID, "api", fmt.Sprintf("threshold-fast-path-%d", i), fixture.createRunArgs,
				); err != nil {
					t.Fatalf("admission %d: %v", i, err)
				}
			}
			if got := starter.begins.Load(); got != tt.wantBegins {
				t.Fatalf("transactions begun = %d, want %d", got, tt.wantBegins)
			}
		})
	}
}

func TestAutopilotQuotaThresholdNoticeFastPathUsesAdmissionSnapshot(t *testing.T) {
	policy := autopilotQuotaPolicy{
		action: entitlement.ActionEnforce,
		notifications: &entitlement.NotificationPolicy{Thresholds: []entitlement.NotificationThreshold{
			{Key: "usage_50", Percent: 50, AtCount: 5},
		}},
	}
	tests := []struct {
		name   string
		period db.AutopilotQuotaPeriod
	}{
		{
			name: "threshold not reached",
			period: db.AutopilotQuotaPeriod{
				ReservedCount: 4,
			},
		},
		{
			name: "threshold already delivered",
			period: db.AutopilotQuotaPeriod{
				ReservedCount: 5, NotifiedThresholds: []byte(`{"usage_50":true}`),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Queries and TxStarter are intentionally nil: a fast-path delivery
			// must decide entirely from the admission snapshot.
			service := &AutopilotService{}
			service.deliverAutopilotQuotaThresholdNotices(
				context.Background(), policy, tt.period, "api", db.CreateAutopilotRunParams{}, pgtype.UUID{},
			)
		})
	}
}

func TestAutopilotQuotaRejectionNoticeAudiences(t *testing.T) {
	fixture := newAutopilotQuotaFixture(t, entitlement.ActionEnforce, 2)
	fixture.setNotificationPolicy(2,
		entitlement.NotificationThreshold{Key: "usage_50", Percent: 50, AtCount: 1},
	)
	adminID := addAutopilotQuotaMember(t, fixture, "admin")
	actorID := addAutopilotQuotaMember(t, fixture, "member")
	subscriberID := addAutopilotQuotaMember(t, fixture, "member")
	uninvolvedID := addAutopilotQuotaMember(t, fixture, "member")
	ctx := context.Background()
	if err := fixture.queries.AddAutopilotSubscriber(ctx, db.AddAutopilotSubscriberParams{
		AutopilotID: fixture.autopilotID, UserType: "member", UserID: subscriberID,
	}); err != nil {
		t.Fatalf("add autopilot subscriber: %v", err)
	}
	t.Cleanup(func() {
		fixture.pool.Exec(context.Background(), `DELETE FROM autopilot_subscriber WHERE autopilot_id = $1`, fixture.autopilotID)
	})

	for i := 1; i <= 2; i++ {
		if _, _, err := fixture.service.createAutopilotRunWithQuota(
			ctx, fixture.workspaceID, "api", fmt.Sprintf("fill-quota-%d", i), fixture.createRunArgs,
		); err != nil {
			t.Fatalf("fill quota %d: %v", i, err)
		}
	}
	noticeNow := fixture.periodStart.Add(time.Hour)
	fixture.service.quotaNoticeNow = func() time.Time { return noticeNow }
	scheduleParams := fixture.createRunArgs
	scheduleParams.Source = "schedule"
	for _, key := range []string{"schedule-first-rejection", "schedule-second-rejection"} {
		_, _, err := fixture.service.createAutopilotRunWithQuota(
			ctx, fixture.workspaceID, "schedule", key, scheduleParams,
		)
		var quotaErr *AutopilotQuotaExceededError
		if !errors.As(err, &quotaErr) {
			t.Fatalf("schedule rejection %q = %v, want quota error", key, err)
		}
	}
	// Repeated automated ticks are coalesced per autopilot. Once Cloud's
	// interval elapses, the affected audience receives the next notice.
	noticeNow = noticeNow.Add(24 * time.Hour)
	_, _, err := fixture.service.createAutopilotRunWithQuota(
		ctx, fixture.workspaceID, "schedule", "schedule-after-interval", scheduleParams,
	)
	var quotaErr *AutopilotQuotaExceededError
	if !errors.As(err, &quotaErr) {
		t.Fatalf("schedule rejection after interval = %v, want quota error", err)
	}
	manualParams := fixture.createRunArgs
	manualParams.Source = "manual"
	for _, key := range []string{"manual-rejection", "manual-rejection-again"} {
		_, _, err = fixture.service.createAutopilotRunWithQuotaForActor(
			ctx, fixture.workspaceID, "manual", key, actorID, manualParams,
		)
		if !errors.As(err, &quotaErr) {
			t.Fatalf("manual rejection %q = %v, want quota error", key, err)
		}
	}
	// API dispatch is also machine-facing, so it shares the per-autopilot
	// interval with schedule and webhook. Move past the schedule marker, then
	// prove an immediate repeated API attempt is coalesced.
	noticeNow = noticeNow.Add(24 * time.Hour)
	apiParams := fixture.createRunArgs
	apiParams.Source = "api"
	for _, key := range []string{"api-rejection", "api-rejection-coalesced"} {
		_, _, err = fixture.service.createAutopilotRunWithQuota(
			ctx, fixture.workspaceID, "api", key, apiParams,
		)
		if !errors.As(err, &quotaErr) {
			t.Fatalf("api rejection %q = %v, want quota error", key, err)
		}
	}

	var firstTitle, firstNoticeKey string
	var firstHasThresholdPercent bool
	if err := fixture.pool.QueryRow(ctx, `
		SELECT details->>'autopilot_title', details->>'notice_key', details ? 'threshold_percent'
		FROM inbox_item
		WHERE workspace_id = $1 AND type = 'autopilot_quota_exceeded'
		  AND details->>'source' = 'schedule'
		  AND details->>'notice_key' = 'rejection_first'
		LIMIT 1`, fixture.workspaceID).Scan(&firstTitle, &firstNoticeKey, &firstHasThresholdPercent); err != nil {
		t.Fatalf("load first rejection details: %v", err)
	}
	if firstTitle == "" || firstNoticeKey != "rejection_first" || firstHasThresholdPercent {
		t.Fatalf("first rejection details = title %q, key %q, threshold field %v", firstTitle, firstNoticeKey, firstHasThresholdPercent)
	}
	var intervalNotices int
	if err := fixture.pool.QueryRow(ctx, `
		SELECT count(*) FROM inbox_item
		WHERE workspace_id = $1 AND type = 'autopilot_quota_exceeded'
		  AND details->>'source' = 'schedule'
		  AND details->>'notice_key' = 'rejection_automated_interval'`, fixture.workspaceID).Scan(&intervalNotices); err != nil {
		t.Fatalf("count interval rejection notices: %v", err)
	}
	if intervalNotices != 3 {
		t.Fatalf("interval rejection rows = %d, want affected creator/admin/subscriber only", intervalNotices)
	}
	var apiIntervalNotices int
	if err := fixture.pool.QueryRow(ctx, `
		SELECT count(*) FROM inbox_item
		WHERE workspace_id = $1 AND type = 'autopilot_quota_exceeded'
		  AND details->>'source' = 'api'
		  AND details->>'notice_key' = 'rejection_automated_interval'`, fixture.workspaceID).Scan(&apiIntervalNotices); err != nil {
		t.Fatalf("count API interval rejection notices: %v", err)
	}
	if apiIntervalNotices != 3 {
		t.Fatalf("API interval rejection rows = %d, want one affected-audience delivery", apiIntervalNotices)
	}

	wantSchedule := map[string]int{
		util.UUIDToString(fixture.publisherID): 2,
		util.UUIDToString(adminID):             2,
		util.UUIDToString(subscriberID):        2,
		util.UUIDToString(actorID):             1,
		util.UUIDToString(uninvolvedID):        1,
	}
	wantManual := map[string]int{
		util.UUIDToString(fixture.publisherID): 2,
		util.UUIDToString(adminID):             2,
		util.UUIDToString(actorID):             2,
		util.UUIDToString(subscriberID):        0,
		util.UUIDToString(uninvolvedID):        0,
	}
	wantAPI := map[string]int{
		util.UUIDToString(fixture.publisherID): 1,
		util.UUIDToString(adminID):             1,
		util.UUIDToString(actorID):             0,
		util.UUIDToString(subscriberID):        1,
		util.UUIDToString(uninvolvedID):        0,
	}
	for recipientID, want := range wantSchedule {
		var got int
		if err := fixture.pool.QueryRow(ctx, `
			SELECT count(*) FROM inbox_item
			WHERE workspace_id = $1 AND recipient_id = $2
			  AND type = 'autopilot_quota_exceeded' AND details->>'source' = 'schedule'`,
			fixture.workspaceID, recipientID).Scan(&got); err != nil {
			t.Fatalf("count schedule notices for %s: %v", recipientID, err)
		}
		if got != want {
			t.Errorf("schedule notices for %s = %d, want %d", recipientID, got, want)
		}
	}
	for recipientID, want := range wantManual {
		var got int
		if err := fixture.pool.QueryRow(ctx, `
			SELECT count(*) FROM inbox_item
			WHERE workspace_id = $1 AND recipient_id = $2
			  AND type = 'autopilot_quota_exceeded' AND details->>'source' = 'manual'`,
			fixture.workspaceID, recipientID).Scan(&got); err != nil {
			t.Fatalf("count manual notices for %s: %v", recipientID, err)
		}
		if got != want {
			t.Errorf("manual notices for %s = %d, want %d", recipientID, got, want)
		}
	}
	for recipientID, want := range wantAPI {
		var got int
		if err := fixture.pool.QueryRow(ctx, `
			SELECT count(*) FROM inbox_item
			WHERE workspace_id = $1 AND recipient_id = $2
			  AND type = 'autopilot_quota_exceeded' AND details->>'source' = 'api'`,
			fixture.workspaceID, recipientID).Scan(&got); err != nil {
			t.Fatalf("count api notices for %s: %v", recipientID, err)
		}
		if got != want {
			t.Errorf("api notices for %s = %d, want %d", recipientID, got, want)
		}
	}
}

func TestAutopilotQuotaMalformedNotificationStateDoesNotBlockAdmission(t *testing.T) {
	fixture := newAutopilotQuotaFixture(t, entitlement.ActionEnforce, 2)
	fixture.setNotificationPolicy(2,
		entitlement.NotificationThreshold{Key: "usage_50", Percent: 50, AtCount: 2},
	)
	ctx := context.Background()
	if _, _, err := fixture.service.createAutopilotRunWithQuota(
		ctx, fixture.workspaceID, "api", "malformed-state-first", fixture.createRunArgs,
	); err != nil {
		t.Fatalf("first admission: %v", err)
	}
	if _, err := fixture.pool.Exec(ctx, `
		UPDATE autopilot_quota_period
		SET notified_thresholds = '[]'::jsonb
		WHERE workspace_id = $1 AND period_start = $2 AND period_end = $3`,
		fixture.workspaceID, fixture.periodStart, fixture.periodEnd); err != nil {
		t.Fatalf("corrupt notification state: %v", err)
	}

	if _, _, err := fixture.service.createAutopilotRunWithQuota(
		ctx, fixture.workspaceID, "api", "malformed-state-second", fixture.createRunArgs,
	); err != nil {
		t.Fatalf("admission with malformed notification state: %v", err)
	}
	var marker bool
	if err := fixture.pool.QueryRow(ctx, `
		SELECT COALESCE((notified_thresholds->>'usage_50')::boolean, false)
		FROM autopilot_quota_period
		WHERE workspace_id = $1 AND period_start = $2 AND period_end = $3`,
		fixture.workspaceID, fixture.periodStart, fixture.periodEnd).Scan(&marker); err != nil {
		t.Fatalf("load repaired notification marker: %v", err)
	}
	if !marker {
		t.Fatal("malformed threshold marker was not repaired after successful admission")
	}
}

func TestAutopilotQuotaInboxFailurePreservesQuotaRejection(t *testing.T) {
	fixture := newAutopilotQuotaFixture(t, entitlement.ActionEnforce, 1)
	fixture.setNotificationPolicy(1)
	ctx := context.Background()
	if _, _, err := fixture.service.createAutopilotRunWithQuota(
		ctx, fixture.workspaceID, "api", "inbox-failure-fill", fixture.createRunArgs,
	); err != nil {
		t.Fatalf("fill quota: %v", err)
	}

	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")
	functionName := pgx.Identifier{"test_fail_quota_notice_" + suffix}.Sanitize()
	triggerName := pgx.Identifier{"test_fail_quota_notice_trigger_" + suffix}.Sanitize()
	if _, err := fixture.pool.Exec(ctx, fmt.Sprintf(`
		CREATE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			IF NEW.workspace_id = '%s'::uuid AND NEW.type = 'autopilot_quota_exceeded' THEN
				RAISE EXCEPTION 'injected quota notice failure';
			END IF;
			RETURN NEW;
		END;
		$$;
		CREATE TRIGGER %s BEFORE INSERT ON inbox_item
		FOR EACH ROW EXECUTE FUNCTION %s();`,
		functionName, fixture.workspace.String(), triggerName, functionName)); err != nil {
		t.Fatalf("install inbox failure trigger: %v", err)
	}
	t.Cleanup(func() {
		fixture.pool.Exec(context.Background(), fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON inbox_item", triggerName))
		fixture.pool.Exec(context.Background(), fmt.Sprintf("DROP FUNCTION IF EXISTS %s()", functionName))
	})

	params := fixture.createRunArgs
	params.Source = "schedule"
	_, _, err := fixture.service.createAutopilotRunWithQuota(
		ctx, fixture.workspaceID, "schedule", "inbox-failure-rejection", params,
	)
	var quotaErr *AutopilotQuotaExceededError
	if !errors.As(err, &quotaErr) {
		t.Fatalf("rejection error = %v, want quota exceeded despite inbox failure", err)
	}
	usage, err := fixture.service.AutopilotQuotaUsage(ctx, fixture.workspaceID)
	if err != nil {
		t.Fatalf("quota usage: %v", err)
	}
	if got := usage.BlockedCounts["schedule"]; got != 1 {
		t.Fatalf("blocked schedule count = %d, want committed count 1", got)
	}
}

func TestAutopilotQuotaConcurrentAdmissionNeverExceedsLimit(t *testing.T) {
	const attempts = 25
	const limit = 5
	fixture := newAutopilotQuotaFixture(t, entitlement.ActionEnforce, limit)
	ctx := context.Background()
	start := make(chan struct{})
	unexpected := make(chan error, attempts)
	var admitted atomic.Int64
	var blocked atomic.Int64
	var wg sync.WaitGroup

	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, _, err := fixture.service.createAutopilotRunWithQuota(
				ctx, fixture.workspaceID, "api", fmt.Sprintf("concurrent-%d", i), fixture.createRunArgs,
			)
			if err == nil {
				admitted.Add(1)
				return
			}
			var quotaErr *AutopilotQuotaExceededError
			if errors.As(err, &quotaErr) {
				blocked.Add(1)
				return
			}
			unexpected <- err
		}(i)
	}
	close(start)
	wg.Wait()
	close(unexpected)
	for err := range unexpected {
		t.Errorf("unexpected concurrent admission error: %v", err)
	}
	if got := admitted.Load(); got != limit {
		t.Fatalf("admitted = %d, want %d", got, limit)
	}
	if got := blocked.Load(); got != attempts-limit {
		t.Fatalf("blocked = %d, want %d", got, attempts-limit)
	}
	usage, err := fixture.service.AutopilotQuotaUsage(ctx, fixture.workspaceID)
	if err != nil || usage.Reserved == nil || *usage.Reserved != limit || *usage.Used != 0 {
		t.Fatalf("concurrent usage = %+v, %v; want %d reserved", usage, err, limit)
	}
}

func TestAutopilotQuotaObserveToEnforceAndOffFinalization(t *testing.T) {
	fixture := newAutopilotQuotaFixture(t, entitlement.ActionObserve, 1)
	metrics := &autopilotQuotaMetricsRecorder{}
	fixture.service.QuotaMetrics = metrics
	ctx := context.Background()
	runs := make([]db.AutopilotRun, 0, 2)
	for i := 0; i < 2; i++ {
		run, _, err := fixture.service.createAutopilotRunWithQuota(
			ctx, fixture.workspaceID, "api", fmt.Sprintf("observe-%d", i), fixture.createRunArgs,
		)
		if err != nil {
			t.Fatalf("observe admission %d: %v", i, err)
		}
		runs = append(runs, run)
	}
	usage, err := fixture.service.AutopilotQuotaUsage(ctx, fixture.workspaceID)
	if err != nil || usage.Action != string(entitlement.ActionObserve) || *usage.Reserved != 2 || usage.Reached != nil {
		t.Fatalf("observe usage = %+v, %v; want two reservations", usage, err)
	}
	if got := metrics.count("observe", "api", "would_block"); got != 1 {
		t.Fatalf("observed would-block metric = %d, want 1", got)
	}

	fixture.setPolicy(entitlement.ActionEnforce, fixture.periodStart, fixture.periodEnd, 1)
	_, _, err = fixture.service.createAutopilotRunWithQuota(
		ctx, fixture.workspaceID, "api", "enforce-after-observe", fixture.createRunArgs,
	)
	var quotaErr *AutopilotQuotaExceededError
	if !errors.As(err, &quotaErr) {
		t.Fatalf("enforce after observe error = %v, want quota exceeded", err)
	}

	// Switching the current policy off must not strand reservations created
	// under observe/enforce. Terminal finalization relies on the persisted slot.
	fixture.setPolicy(entitlement.ActionOff, fixture.periodStart, fixture.periodEnd, 1)
	for _, run := range runs {
		if _, err := fixture.service.failAutopilotRun(ctx, db.UpdateAutopilotRunFailedParams{
			ID: run.ID, FailureReason: pgtype.Text{String: "test cleanup", Valid: true},
		}); err != nil {
			t.Fatalf("finalize while policy off: %v", err)
		}
	}
	fixture.setPolicy(entitlement.ActionEnforce, fixture.periodStart, fixture.periodEnd, 1)
	usage, err = fixture.service.AutopilotQuotaUsage(ctx, fixture.workspaceID)
	if err != nil || *usage.Used != 0 || *usage.Reserved != 0 {
		t.Fatalf("usage after off finalization = %+v, %v; want zero", usage, err)
	}
	if _, _, err := fixture.service.createAutopilotRunWithQuota(
		ctx, fixture.workspaceID, "api", "reopened-after-off", fixture.createRunArgs,
	); err != nil {
		t.Fatalf("admission after reopening policy: %v", err)
	}
}

func TestAutopilotQuotaConsumedCreateIssueCannotBeRefunded(t *testing.T) {
	fixture := newAutopilotQuotaFixture(t, entitlement.ActionEnforce, 2)
	ctx := context.Background()

	cancelled, _, err := fixture.service.createAutopilotRunWithQuota(
		ctx, fixture.workspaceID, "api", "consumed-then-cancelled", fixture.createRunArgs,
	)
	if err != nil {
		t.Fatalf("admit cancelled run: %v", err)
	}
	if _, err := settleAutopilotQuota(ctx, fixture.queries, cancelled.QuotaReservationID, true); err != nil {
		t.Fatalf("consume cancelled run: %v", err)
	}
	if _, err := fixture.service.failAutopilotRun(ctx, db.UpdateAutopilotRunFailedParams{
		ID: cancelled.ID, FailureReason: pgtype.Text{String: "issue cancelled", Valid: true},
	}); err != nil {
		t.Fatalf("fail consumed run: %v", err)
	}

	deleted, _, err := fixture.service.createAutopilotRunWithQuota(
		ctx, fixture.workspaceID, "api", "consumed-then-deleted", fixture.createRunArgs,
	)
	if err != nil {
		t.Fatalf("admit deleted run: %v", err)
	}
	deleted, err = fixture.queries.UpdateAutopilotRunIssueCreated(ctx, db.UpdateAutopilotRunIssueCreatedParams{
		ID: deleted.ID, IssueID: fixture.issueID,
	})
	if err != nil {
		t.Fatalf("link issue-created run: %v", err)
	}
	if _, err := settleAutopilotQuota(ctx, fixture.queries, deleted.QuotaReservationID, true); err != nil {
		t.Fatalf("consume deleted run: %v", err)
	}
	if err := fixture.service.FailAutopilotRunsByIssue(ctx, fixture.issueID); err != nil {
		t.Fatalf("fail runs before issue deletion: %v", err)
	}

	usage, err := fixture.service.AutopilotQuotaUsage(ctx, fixture.workspaceID)
	if err != nil || *usage.Used != 2 || *usage.Reserved != 0 {
		t.Fatalf("usage after cancellation/deletion = %+v, %v; want two consumed units", usage, err)
	}
}

func TestAutopilotQuotaTerminalUpdateNeedsNoTransactionStarter(t *testing.T) {
	fixture := newAutopilotQuotaFixture(t, entitlement.ActionOff, 1)
	ctx := context.Background()
	run, err := fixture.queries.CreateAutopilotRun(ctx, fixture.createRunArgs)
	if err != nil {
		t.Fatalf("create off-path run: %v", err)
	}
	serviceWithoutTx := &AutopilotService{Queries: fixture.queries}
	updated, err := serviceWithoutTx.completeAutopilotRun(ctx, db.UpdateAutopilotRunCompletedParams{ID: run.ID})
	if err != nil {
		t.Fatalf("complete off-path run: %v", err)
	}
	if updated.Status != "completed" || updated.QuotaReservationID.Valid {
		t.Fatalf("completed off-path run = %+v", updated)
	}
}

func TestAutopilotQuotaSettlementStaysInOriginalPeriod(t *testing.T) {
	fixture := newAutopilotQuotaFixture(t, entitlement.ActionEnforce, 1)
	ctx := context.Background()
	run, _, err := fixture.service.createAutopilotRunWithQuota(
		ctx, fixture.workspaceID, "api", "cross-period", fixture.createRunArgs,
	)
	if err != nil {
		t.Fatalf("admit first-period run: %v", err)
	}
	secondStart := fixture.periodEnd
	secondEnd := secondStart.Add(19 * time.Hour)
	fixture.setPolicy(entitlement.ActionEnforce, secondStart, secondEnd, 1)
	if _, err := fixture.service.completeAutopilotRun(ctx, db.UpdateAutopilotRunCompletedParams{ID: run.ID}); err != nil {
		t.Fatalf("complete after period rollover: %v", err)
	}

	first, err := fixture.queries.GetAutopilotQuotaPeriod(ctx, db.GetAutopilotQuotaPeriodParams{
		WorkspaceID: fixture.workspaceID,
		PeriodStart: pgtype.Timestamptz{Time: fixture.periodStart, Valid: true},
		PeriodEnd:   pgtype.Timestamptz{Time: fixture.periodEnd, Valid: true},
	})
	if err != nil || first.UsedCount != 1 || first.ReservedCount != 0 {
		t.Fatalf("first period = %+v, %v; want one consumed", first, err)
	}
	second, err := fixture.service.AutopilotQuotaUsage(ctx, fixture.workspaceID)
	if err != nil || *second.Used != 0 || *second.Reserved != 0 {
		t.Fatalf("second period usage = %+v, %v; want zero", second, err)
	}
	if _, _, err := fixture.service.createAutopilotRunWithQuota(
		ctx, fixture.workspaceID, "api", "second-period", fixture.createRunArgs,
	); err != nil {
		t.Fatalf("admit second-period run: %v", err)
	}
}

func TestAutopilotQuotaWorkspaceIsolation(t *testing.T) {
	first := newAutopilotQuotaFixture(t, entitlement.ActionEnforce, 1)
	second := newAutopilotQuotaFixture(t, entitlement.ActionEnforce, 1)
	ctx := context.Background()
	if _, _, err := first.service.createAutopilotRunWithQuota(
		ctx, first.workspaceID, "api", "isolated", first.createRunArgs,
	); err != nil {
		t.Fatalf("first workspace admission: %v", err)
	}
	_, _, err := first.service.createAutopilotRunWithQuota(
		ctx, first.workspaceID, "api", "isolated-over-limit", first.createRunArgs,
	)
	var quotaErr *AutopilotQuotaExceededError
	if !errors.As(err, &quotaErr) {
		t.Fatalf("first workspace over-limit error = %v", err)
	}
	if _, _, err := second.service.createAutopilotRunWithQuota(
		ctx, second.workspaceID, "api", "isolated", second.createRunArgs,
	); err != nil {
		t.Fatalf("second workspace admission leaked first usage: %v", err)
	}
}

func TestAutopilotQuotaReconcilerSettlesTerminalOnlyOnce(t *testing.T) {
	fixture := newAutopilotQuotaFixture(t, entitlement.ActionEnforce, 2)
	ctx := context.Background()
	terminal, _, err := fixture.service.createAutopilotRunWithQuota(
		ctx, fixture.workspaceID, "api", "reconcile-terminal", fixture.createRunArgs,
	)
	if err != nil {
		t.Fatalf("admit terminal run: %v", err)
	}
	active, _, err := fixture.service.createAutopilotRunWithQuota(
		ctx, fixture.workspaceID, "api", "reconcile-active", fixture.createRunArgs,
	)
	if err != nil {
		t.Fatalf("admit active run: %v", err)
	}
	// Simulate a crash after the run reached terminal state but before the old
	// two-step finalizer settled its reservation.
	if _, err := fixture.queries.UpdateAutopilotRunCompleted(ctx, db.UpdateAutopilotRunCompletedParams{ID: terminal.ID}); err != nil {
		t.Fatalf("seed terminal crash window: %v", err)
	}

	type reconcileResult struct {
		settled int
		err     error
	}
	results := make(chan reconcileResult, 2)
	for i := 0; i < 2; i++ {
		go func() {
			settled, err := fixture.service.ReconcileAutopilotQuotaReservations(
				ctx, time.Now().Add(time.Minute), time.Now().Add(-6*time.Hour), 10,
			)
			results <- reconcileResult{settled: settled, err: err}
		}()
	}
	total := 0
	for i := 0; i < 2; i++ {
		result := <-results
		if result.err != nil {
			t.Fatalf("reconcile replica %d: %v", i, result.err)
		}
		total += result.settled
	}
	if total != 1 {
		t.Fatalf("replicas reported %d settlements, want exactly 1", total)
	}
	activeAfter, err := fixture.queries.GetAutopilotRun(ctx, active.ID)
	if err != nil || activeAfter.Status != "running" {
		t.Fatalf("active run after reconcile = %+v, %v; want running", activeAfter, err)
	}
	usage, err := fixture.service.AutopilotQuotaUsage(ctx, fixture.workspaceID)
	if err != nil || *usage.Used != 1 || *usage.Reserved != 1 {
		t.Fatalf("reconciled usage = %+v, %v; want one used/one reserved", usage, err)
	}
}

func TestAutopilotQuotaReconcilerReleasesOnlyAbandonedManualOrAPIRuns(t *testing.T) {
	fixture := newAutopilotQuotaFixture(t, entitlement.ActionEnforce, 4)
	ctx := context.Background()
	create := func(source, key string) db.AutopilotRun {
		t.Helper()
		params := fixture.createRunArgs
		params.Source = source
		run, _, err := fixture.service.createAutopilotRunWithQuota(
			ctx, fixture.workspaceID, source, key, params,
		)
		if err != nil {
			t.Fatalf("admit %s partial run: %v", source, err)
		}
		return run
	}

	staleManual := create("manual", "stale-manual")
	taskBackedAPI := create("api", "task-backed-api")
	staleWebhook := create("webhook", "stale-webhook")
	freshAPI := create("api", "fresh-api")
	agent, err := fixture.queries.GetAgent(ctx, fixture.agentID)
	if err != nil {
		t.Fatalf("load task-backed run agent: %v", err)
	}
	if _, err := fixture.queries.CreateAutopilotTask(ctx, db.CreateAutopilotTaskParams{
		AgentID:              fixture.agentID,
		RuntimeID:            agent.RuntimeID,
		AutopilotRunID:       taskBackedAPI.ID,
		OriginatorUserID:     fixture.publisherID,
		AccountableUserID:    fixture.publisherID,
		OriginatorSource:     pgtype.Text{String: "direct_human", Valid: true},
		TriggerEvidenceKind:  pgtype.Text{String: "autopilot_run", Valid: true},
		TriggerEvidenceRefID: taskBackedAPI.ID,
	}); err != nil {
		t.Fatalf("create unlinked task evidence: %v", err)
	}
	if _, err := fixture.pool.Exec(ctx, `
		UPDATE autopilot_quota_reservation
		SET created_at = now() - interval '24 hours'
		WHERE id = ANY($1::uuid[])`, []uuid.UUID{
		uuid.UUID(staleManual.QuotaReservationID.Bytes),
		uuid.UUID(taskBackedAPI.QuotaReservationID.Bytes),
		uuid.UUID(staleWebhook.QuotaReservationID.Bytes),
	}); err != nil {
		t.Fatalf("age partial reservations: %v", err)
	}

	now := time.Now()
	settled, err := fixture.service.ReconcileAutopilotQuotaReservations(
		ctx, now.Add(-10*time.Minute), now.Add(-6*time.Hour), 10,
	)
	if err != nil || settled != 1 {
		t.Fatalf("reconcile abandoned partial runs = %d, %v; want one", settled, err)
	}

	manualAfter, err := fixture.queries.GetAutopilotRun(ctx, staleManual.ID)
	if err != nil || manualAfter.Status != "failed" {
		t.Fatalf("stale manual run = %+v, %v; want failed", manualAfter, err)
	}
	for name, run := range map[string]db.AutopilotRun{
		"durable webhook": staleWebhook,
		"task-backed api": taskBackedAPI,
		"fresh api":       freshAPI,
	} {
		after, err := fixture.queries.GetAutopilotRun(ctx, run.ID)
		if err != nil || after.Status != "running" {
			t.Fatalf("%s run = %+v, %v; want running", name, after, err)
		}
	}
	usage, err := fixture.service.AutopilotQuotaUsage(ctx, fixture.workspaceID)
	if err != nil || usage.Reserved == nil || *usage.Reserved != 3 || *usage.Used != 0 {
		t.Fatalf("usage after partial recovery = %+v, %v; want three reservations", usage, err)
	}
}

func TestAutopilotQuotaIdempotencyRecoversOrphanedReservation(t *testing.T) {
	fixture := newAutopilotQuotaFixture(t, entitlement.ActionEnforce, 1)
	ctx := context.Background()
	first, _, err := fixture.service.createAutopilotRunWithQuota(
		ctx, fixture.workspaceID, "api", "orphaned-key", fixture.createRunArgs,
	)
	if err != nil {
		t.Fatalf("first admission: %v", err)
	}
	if _, err := fixture.pool.Exec(ctx, `DELETE FROM autopilot_run WHERE id = $1`, first.ID); err != nil {
		t.Fatalf("remove quota-linked run: %v", err)
	}
	recovered, reused, err := fixture.service.createAutopilotRunWithQuota(
		ctx, fixture.workspaceID, "api", "orphaned-key", fixture.createRunArgs,
	)
	if err != nil || reused || recovered.ID.Bytes == first.ID.Bytes {
		t.Fatalf("orphan recovery = run %s reused=%v err=%v", util.UUIDToString(recovered.ID), reused, err)
	}
	usage, err := fixture.service.AutopilotQuotaUsage(ctx, fixture.workspaceID)
	if err != nil || *usage.Used != 0 || *usage.Reserved != 1 {
		t.Fatalf("orphan-recovered usage = %+v, %v; want one reservation", usage, err)
	}
}

func TestAutopilotQuotaSchedulePersistsSkippedRun(t *testing.T) {
	pool := newResolveOriginatorPool(t)
	ctx := context.Background()
	q := db.New(pool)
	workspaceIDString, publisherID, agentID, _ := seedAttributionFixture(t, pool)
	autopilotIDString, _ := seedRunOnlyAutopilot(t, pool, workspaceIDString, agentID, publisherID)
	workspaceID := util.MustParseUUID(workspaceIDString)
	t.Cleanup(func() {
		pool.Exec(context.Background(), `DELETE FROM autopilot_quota_reservation WHERE workspace_id = $1`, workspaceID)
		pool.Exec(context.Background(), `DELETE FROM autopilot_quota_period WHERE workspace_id = $1`, workspaceID)
	})

	start := time.Now().UTC().Truncate(time.Second)
	end := start.Add(13 * time.Hour)
	limit := 0
	stub := entitlementtest.New()
	stub.Set(uuid.UUID(workspaceID.Bytes), entitlement.GateAutopilotRuns, entitlement.Decision{
		Gate: entitlement.Gate{
			Action: entitlement.ActionEnforce, Limit: &limit,
			PeriodStart: &start, PeriodEnd: &end, ResetAt: &end,
		},
	})
	autopilot, err := q.GetAutopilot(ctx, util.MustParseUUID(autopilotIDString))
	if err != nil {
		t.Fatalf("load autopilot: %v", err)
	}
	var triggerID pgtype.UUID
	if err := pool.QueryRow(ctx, `
		INSERT INTO autopilot_trigger (autopilot_id, kind, enabled, cron_expression, timezone)
		VALUES ($1, 'schedule', true, '0 * * * *', 'UTC') RETURNING id`, autopilot.ID).Scan(&triggerID); err != nil {
		t.Fatalf("create schedule trigger: %v", err)
	}
	svc := &AutopilotService{
		Queries: q, TxStarter: pool, Bus: events.New(),
		TaskSvc:      &TaskService{Queries: q, TxStarter: pool, Bus: events.New()},
		Entitlements: stub,
	}
	run, err := svc.DispatchAutopilotForPlan(ctx, autopilot, triggerID, "schedule", nil, start.Add(time.Minute))
	if err != nil {
		t.Fatalf("scheduled dispatch: %v", err)
	}
	if run.Status != "skipped" || !run.ReasonCode.Valid || run.ReasonCode.String != "quota_exceeded" {
		t.Fatalf("scheduled run = status %q reason %q, want skipped/quota_exceeded", run.Status, run.ReasonCode.String)
	}
}
