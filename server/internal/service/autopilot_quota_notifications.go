package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/entitlement"
	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/dbid"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

const autopilotQuotaFirstRejectionNoticeKey = "rejection_first"

const (
	autopilotQuotaRejectionAttemptNoticeKey  = "rejection_attempt"
	autopilotQuotaRejectionIntervalNoticeKey = "rejection_automated_interval"
)

type autopilotQuotaNoticeAudience int

const (
	autopilotQuotaNoticeAllMembers autopilotQuotaNoticeAudience = iota
	autopilotQuotaNoticeAffectedMembers
)

type autopilotQuotaNoticeFacts struct {
	Kind             string
	NoticeKey        string
	ThresholdPercent int
	WorkspaceID      pgtype.UUID
	AutopilotID      pgtype.UUID
	ActorUserID      pgtype.UUID
	Source           string
	Used             int64
	Reserved         int64
	Total            int64
	Limit            int64
	ResetAt          time.Time
}

func (s *AutopilotService) autopilotQuotaNoticeNow() time.Time {
	if s.quotaNoticeNow != nil {
		return s.quotaNoticeNow().UTC()
	}
	return time.Now().UTC()
}

func (s *AutopilotService) deliverAutopilotQuotaThresholdNotices(
	ctx context.Context,
	policy autopilotQuotaPolicy,
	workspaceID pgtype.UUID,
	source string,
	params db.CreateAutopilotRunParams,
	actorUserID pgtype.UUID,
) {
	if policy.action != entitlement.ActionEnforce || policy.notifications == nil {
		return
	}
	items, err := s.createAutopilotQuotaThresholdNotices(ctx, policy, workspaceID, source, params, actorUserID)
	if err != nil {
		slog.Warn("autopilot quota threshold notice delivery failed; admission remains committed",
			"workspace_id", util.UUIDToString(workspaceID),
			"autopilot_id", util.UUIDToString(params.AutopilotID),
			"error", err,
		)
		return
	}
	s.publishAutopilotQuotaInboxItems(items)
}

func (s *AutopilotService) createAutopilotQuotaThresholdNotices(
	ctx context.Context,
	policy autopilotQuotaPolicy,
	workspaceID pgtype.UUID,
	source string,
	params db.CreateAutopilotRunParams,
	actorUserID pgtype.UUID,
) ([]db.InboxItem, error) {
	tx, err := s.TxStarter.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin quota threshold notice: %w", err)
	}
	defer tx.Rollback(ctx)
	q := s.Queries.WithTx(tx)
	period, err := q.EnsureAutopilotQuotaPeriod(ctx, db.EnsureAutopilotQuotaPeriodParams{
		WorkspaceID: workspaceID,
		PeriodStart: pgtype.Timestamptz{Time: policy.periodStart, Valid: true},
		PeriodEnd:   pgtype.Timestamptz{Time: policy.periodEnd, Valid: true},
	})
	if err != nil {
		return nil, fmt.Errorf("lock quota period for threshold notice: %w", err)
	}

	items := make([]db.InboxItem, 0)
	for _, threshold := range policy.notifications.Thresholds {
		if period.UsedCount+period.ReservedCount < int64(threshold.AtCount) {
			continue
		}
		if autopilotQuotaThresholdWasNotified(period, threshold.Key) {
			continue
		}

		created, err := s.createAutopilotQuotaInboxItems(ctx, q, autopilotQuotaNoticeAllMembers, autopilotQuotaNoticeFacts{
			Kind: "threshold", NoticeKey: threshold.Key, ThresholdPercent: threshold.Percent,
			WorkspaceID: period.WorkspaceID, AutopilotID: params.AutopilotID, ActorUserID: actorUserID,
			Source: source, Used: period.UsedCount, Reserved: period.ReservedCount,
			Total: period.UsedCount + period.ReservedCount, Limit: policy.limit, ResetAt: policy.resetAt,
		})
		if err != nil {
			return nil, err
		}
		if len(created) == 0 {
			continue
		}
		period, err = q.MarkAutopilotQuotaThresholdNotified(ctx, db.MarkAutopilotQuotaThresholdNotifiedParams{
			ThresholdKey: threshold.Key,
			WorkspaceID:  period.WorkspaceID,
			PeriodStart:  period.PeriodStart,
			PeriodEnd:    period.PeriodEnd,
		})
		if err != nil {
			return nil, fmt.Errorf("mark autopilot quota threshold notified: %w", err)
		}
		items = append(items, created...)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit quota threshold notices: %w", err)
	}
	return items, nil
}

func (s *AutopilotService) deliverAutopilotQuotaRejectionNotices(
	ctx context.Context,
	policy autopilotQuotaPolicy,
	workspaceID pgtype.UUID,
	source string,
	params db.CreateAutopilotRunParams,
	actorUserID pgtype.UUID,
) {
	if policy.action != entitlement.ActionEnforce || policy.notifications == nil ||
		policy.notifications.OnRejection != entitlement.NotificationEveryAttempt {
		return
	}
	items, err := s.createAutopilotQuotaRejectionNotices(ctx, policy, workspaceID, source, params, actorUserID)
	if err != nil {
		slog.Warn("autopilot quota rejection notice delivery failed; quota rejection remains committed",
			"workspace_id", util.UUIDToString(workspaceID),
			"autopilot_id", util.UUIDToString(params.AutopilotID),
			"source", source,
			"error", err,
		)
		return
	}
	s.publishAutopilotQuotaInboxItems(items)
}

func (s *AutopilotService) createAutopilotQuotaRejectionNotices(
	ctx context.Context,
	policy autopilotQuotaPolicy,
	workspaceID pgtype.UUID,
	source string,
	params db.CreateAutopilotRunParams,
	actorUserID pgtype.UUID,
) ([]db.InboxItem, error) {
	tx, err := s.TxStarter.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin quota rejection notice: %w", err)
	}
	defer tx.Rollback(ctx)
	q := s.Queries.WithTx(tx)
	period, err := q.EnsureAutopilotQuotaPeriod(ctx, db.EnsureAutopilotQuotaPeriodParams{
		WorkspaceID: workspaceID,
		PeriodStart: pgtype.Timestamptz{Time: policy.periodStart, Valid: true},
		PeriodEnd:   pgtype.Timestamptz{Time: policy.periodEnd, Valid: true},
	})
	if err != nil {
		return nil, fmt.Errorf("lock quota period for rejection notice: %w", err)
	}

	firstAlreadyNotified := autopilotQuotaThresholdWasNotified(period, autopilotQuotaFirstRejectionNoticeKey)
	automated := source == "schedule" || source == "webhook"
	notifiedAt := s.autopilotQuotaNoticeNow()
	if automated && firstAlreadyNotified {
		lastNotifiedAt, ok := autopilotQuotaAutomatedRejectionNotifiedAt(period, params.AutopilotID)
		if ok && notifiedAt.Before(lastNotifiedAt.Add(policy.notifications.AutomatedRejectionMinInterval)) {
			if err := tx.Commit(ctx); err != nil {
				return nil, fmt.Errorf("commit throttled quota rejection notice: %w", err)
			}
			return nil, nil
		}
	}

	audience := autopilotQuotaNoticeAffectedMembers
	noticeKey := autopilotQuotaRejectionAttemptNoticeKey
	if !firstAlreadyNotified {
		audience = autopilotQuotaNoticeAllMembers
		noticeKey = autopilotQuotaFirstRejectionNoticeKey
	} else if automated {
		noticeKey = autopilotQuotaRejectionIntervalNoticeKey
	}
	items, err := s.createAutopilotQuotaInboxItems(ctx, q, audience, autopilotQuotaNoticeFacts{
		Kind: "rejected", NoticeKey: noticeKey,
		WorkspaceID: period.WorkspaceID, AutopilotID: params.AutopilotID, ActorUserID: actorUserID,
		Source: source, Used: period.UsedCount, Reserved: period.ReservedCount,
		Total: period.UsedCount + period.ReservedCount, Limit: policy.limit, ResetAt: policy.resetAt,
	})
	if err != nil {
		return nil, err
	}
	if !firstAlreadyNotified && len(items) > 0 {
		period, err = q.MarkAutopilotQuotaThresholdNotified(ctx, db.MarkAutopilotQuotaThresholdNotifiedParams{
			ThresholdKey: autopilotQuotaFirstRejectionNoticeKey,
			WorkspaceID:  period.WorkspaceID,
			PeriodStart:  period.PeriodStart,
			PeriodEnd:    period.PeriodEnd,
		})
		if err != nil {
			return nil, fmt.Errorf("mark first autopilot quota rejection notified: %w", err)
		}
	}
	if automated && len(items) > 0 {
		period, err = q.MarkAutopilotQuotaAutomatedRejectionNotified(ctx, db.MarkAutopilotQuotaAutomatedRejectionNotifiedParams{
			AutopilotKey: util.UUIDToString(params.AutopilotID),
			NotifiedAt:   pgtype.Timestamptz{Time: notifiedAt, Valid: true},
			WorkspaceID:  period.WorkspaceID,
			PeriodStart:  period.PeriodStart,
			PeriodEnd:    period.PeriodEnd,
		})
		if err != nil {
			return nil, fmt.Errorf("mark automated autopilot quota rejection notified: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit quota rejection notices: %w", err)
	}
	return items, nil
}

func autopilotQuotaThresholdWasNotified(period db.AutopilotQuotaPeriod, key string) bool {
	if len(period.NotifiedThresholds) == 0 {
		return false
	}
	var notified map[string]json.RawMessage
	if err := json.Unmarshal(period.NotifiedThresholds, &notified); err != nil {
		slog.Warn("autopilot quota threshold notification state is malformed; treating it as empty",
			"workspace_id", util.UUIDToString(period.WorkspaceID), "error", err)
		return false
	}
	var value bool
	raw, ok := notified[key]
	if !ok {
		return false
	}
	if err := json.Unmarshal(raw, &value); err != nil {
		slog.Warn("autopilot quota threshold notification marker is malformed; treating it as unset",
			"workspace_id", util.UUIDToString(period.WorkspaceID), "notice_key", key, "error", err)
		return false
	}
	return value
}

func autopilotQuotaAutomatedRejectionNotifiedAt(period db.AutopilotQuotaPeriod, autopilotID pgtype.UUID) (time.Time, bool) {
	if len(period.AutomatedRejectionNotifiedAt) == 0 {
		return time.Time{}, false
	}
	var notified map[string]json.RawMessage
	if err := json.Unmarshal(period.AutomatedRejectionNotifiedAt, &notified); err != nil {
		slog.Warn("automated autopilot quota rejection notification state is malformed; treating it as empty",
			"workspace_id", util.UUIDToString(period.WorkspaceID), "error", err)
		return time.Time{}, false
	}
	key := util.UUIDToString(autopilotID)
	raw, ok := notified[key]
	if !ok {
		return time.Time{}, false
	}
	var value time.Time
	if err := json.Unmarshal(raw, &value); err != nil {
		slog.Warn("automated autopilot quota rejection notification marker is malformed; treating it as unset",
			"workspace_id", util.UUIDToString(period.WorkspaceID), "autopilot_id", key, "error", err)
		return time.Time{}, false
	}
	return value.UTC(), true
}

func (s *AutopilotService) createAutopilotQuotaInboxItems(
	ctx context.Context,
	q *db.Queries,
	audience autopilotQuotaNoticeAudience,
	facts autopilotQuotaNoticeFacts,
) ([]db.InboxItem, error) {
	recipients, autopilotTitle, err := autopilotQuotaNoticeRecipients(
		ctx, q, facts.WorkspaceID, facts.AutopilotID, facts.Source, facts.ActorUserID, audience,
	)
	if err != nil {
		return nil, err
	}
	if len(recipients) == 0 {
		return nil, nil
	}

	title := fmt.Sprintf("Autopilot usage reached %d%%", facts.ThresholdPercent)
	body := fmt.Sprintf(
		"This workspace has used or reserved %d of %d autopilot runs for the current period. The allowance resets at %s.",
		facts.Total, facts.Limit, facts.ResetAt.UTC().Format(time.RFC3339),
	)
	typeName := "autopilot_quota_warning"
	severity := "info"
	if facts.ThresholdPercent >= 80 {
		severity = "attention"
	}
	if facts.Kind == "rejected" {
		title = "Autopilot run limit reached"
		body = fmt.Sprintf(
			"This workspace has reached its limit of %d autopilot runs for the current period. This execution was not started. The allowance resets at %s.",
			facts.Limit, facts.ResetAt.UTC().Format(time.RFC3339),
		)
		if autopilotTitle != "" {
			body = fmt.Sprintf(
				"Autopilot %q was not started because this workspace has reached its limit of %d runs for the current period. The allowance resets at %s.",
				autopilotTitle, facts.Limit, facts.ResetAt.UTC().Format(time.RFC3339),
			)
		}
		typeName = "autopilot_quota_exceeded"
		severity = "action_required"
	}

	// title/body are complete English fallbacks for clients and future delivery
	// channels that do not localize structured details. In-app clients render
	// localized quota copy from these facts.
	detailValues := map[string]string{
		"notice_kind":     facts.Kind,
		"notice_key":      facts.NoticeKey,
		"used":            strconv.FormatInt(facts.Used, 10),
		"reserved":        strconv.FormatInt(facts.Reserved, 10),
		"total":           strconv.FormatInt(facts.Total, 10),
		"limit":           strconv.FormatInt(facts.Limit, 10),
		"reset_at":        facts.ResetAt.UTC().Format(time.RFC3339),
		"source":          facts.Source,
		"autopilot_id":    util.UUIDToString(facts.AutopilotID),
		"autopilot_title": autopilotTitle,
	}
	if facts.Kind == "threshold" {
		detailValues["threshold_percent"] = strconv.Itoa(facts.ThresholdPercent)
	}
	details, err := json.Marshal(detailValues)
	if err != nil {
		return nil, fmt.Errorf("marshal autopilot quota inbox details: %w", err)
	}

	items := make([]db.InboxItem, 0, len(recipients))
	for _, recipientID := range recipients {
		item, err := q.CreateInboxItem(ctx, db.CreateInboxItemParams{
			ID: dbid.NewV7(), WorkspaceID: facts.WorkspaceID,
			RecipientType: "member", RecipientID: recipientID,
			Type: typeName, Severity: severity, IssueID: pgtype.UUID{},
			Title: title, Body: pgtype.Text{String: body, Valid: true},
			ActorType: pgtype.Text{String: "system", Valid: true}, ActorID: pgtype.UUID{},
			Details: details,
		})
		if err != nil {
			return nil, fmt.Errorf("create autopilot quota inbox item: %w", err)
		}
		items = append(items, item)
	}
	return items, nil
}

func autopilotQuotaNoticeRecipients(
	ctx context.Context,
	q *db.Queries,
	workspaceID, autopilotID pgtype.UUID,
	source string,
	actorUserID pgtype.UUID,
	audience autopilotQuotaNoticeAudience,
) ([]pgtype.UUID, string, error) {
	members, err := q.ListMembers(ctx, workspaceID)
	if err != nil {
		return nil, "", fmt.Errorf("list autopilot quota notice members: %w", err)
	}
	selected := make(map[string]bool, len(members))
	memberByID := make(map[string]db.Member, len(members))
	for _, member := range members {
		id := util.UUIDToString(member.UserID)
		memberByID[id] = member
		if audience == autopilotQuotaNoticeAllMembers || member.Role == "owner" || member.Role == "admin" {
			selected[id] = true
		}
	}
	autopilot, err := q.GetAutopilot(ctx, autopilotID)
	if err != nil {
		return nil, "", fmt.Errorf("load autopilot for quota notice: %w", err)
	}
	if audience == autopilotQuotaNoticeAllMembers {
		return selectedAutopilotQuotaRecipients(members, selected), autopilot.Title, nil
	}

	directActorSelected := false
	if (source == "manual" || source == "api") && actorUserID.Valid {
		if id := util.UUIDToString(actorUserID); memberByID[id].UserID.Valid {
			selected[id] = true
			directActorSelected = true
		}
	}

	// API and legacy manual calls do not always carry a member actor. In that
	// case, use the automated-trigger audience instead of notifying managers
	// alone, so the autopilot's current stakeholders still see each rejection.
	autopilotStakeholders := source == "schedule" || source == "webhook" ||
		((source == "manual" || source == "api") && !directActorSelected)
	if autopilotStakeholders {
		switch autopilot.CreatedByType {
		case "member":
			if id := util.UUIDToString(autopilot.CreatedByID); memberByID[id].UserID.Valid {
				selected[id] = true
			}
		case "agent":
			agent, agentErr := q.GetAgent(ctx, autopilot.CreatedByID)
			if agentErr == nil && agent.OwnerID.Valid {
				if id := util.UUIDToString(agent.OwnerID); memberByID[id].UserID.Valid {
					selected[id] = true
				}
			}
		}
		subscribers, err := q.ListAutopilotSubscribers(ctx, autopilotID)
		if err != nil {
			return nil, "", fmt.Errorf("list rejected autopilot subscribers for quota notice: %w", err)
		}
		for _, subscriber := range subscribers {
			if subscriber.UserType != "member" {
				continue
			}
			if id := util.UUIDToString(subscriber.UserID); memberByID[id].UserID.Valid {
				selected[id] = true
			}
		}
	}

	return selectedAutopilotQuotaRecipients(members, selected), autopilot.Title, nil
}

func selectedAutopilotQuotaRecipients(members []db.Member, selected map[string]bool) []pgtype.UUID {
	recipients := make([]pgtype.UUID, 0, len(selected))
	for _, member := range members {
		if selected[util.UUIDToString(member.UserID)] {
			recipients = append(recipients, member.UserID)
		}
	}
	return recipients
}

func (s *AutopilotService) publishAutopilotQuotaInboxItems(items []db.InboxItem) {
	if s.Bus == nil {
		return
	}
	for _, item := range items {
		s.Bus.Publish(events.Event{
			Type: protocol.EventInboxNew, WorkspaceID: util.UUIDToString(item.WorkspaceID),
			ActorType: "system",
			Payload: map[string]any{"item": map[string]any{
				"id": util.UUIDToString(item.ID), "workspace_id": util.UUIDToString(item.WorkspaceID),
				"recipient_type": item.RecipientType, "recipient_id": util.UUIDToString(item.RecipientID),
				"type": item.Type, "severity": item.Severity, "issue_id": nil,
				"issue_status": nil, "issue_priority": nil,
				"title": item.Title, "body": util.TextToPtr(item.Body), "read": item.Read,
				"archived": item.Archived, "created_at": util.TimestampToString(item.CreatedAt),
				"actor_type": util.TextToPtr(item.ActorType), "actor_id": nil,
				"details": json.RawMessage(item.Details),
			}},
		})
	}
}
