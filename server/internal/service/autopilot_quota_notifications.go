package service

import (
	"context"
	"encoding/json"
	"fmt"
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

func (s *AutopilotService) createAutopilotQuotaThresholdNotices(
	ctx context.Context,
	q *db.Queries,
	policy autopilotQuotaPolicy,
	period db.AutopilotQuotaPeriod,
	previousTotal int64,
	params db.CreateAutopilotRunParams,
	actorUserID pgtype.UUID,
) ([]db.InboxItem, db.AutopilotQuotaPeriod, error) {
	if policy.action != entitlement.ActionEnforce || policy.notifications == nil {
		return nil, period, nil
	}

	items := make([]db.InboxItem, 0)
	for _, threshold := range policy.notifications.Thresholds {
		if previousTotal >= int64(threshold.AtCount) ||
			period.UsedCount+period.ReservedCount < int64(threshold.AtCount) {
			continue
		}
		notified, err := autopilotQuotaThresholdWasNotified(period, threshold.Key)
		if err != nil {
			return nil, period, err
		}
		if notified {
			continue
		}

		created, err := s.createAutopilotQuotaInboxItems(ctx, q, autopilotQuotaNoticeAllMembers, autopilotQuotaNoticeFacts{
			Kind: "threshold", NoticeKey: threshold.Key, ThresholdPercent: threshold.Percent,
			WorkspaceID: period.WorkspaceID, AutopilotID: params.AutopilotID, ActorUserID: actorUserID,
			Source: params.Source, Used: period.UsedCount, Reserved: period.ReservedCount,
			Total: period.UsedCount + period.ReservedCount, Limit: policy.limit, ResetAt: policy.resetAt,
		})
		if err != nil {
			return nil, period, err
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
			return nil, period, fmt.Errorf("mark autopilot quota threshold notified: %w", err)
		}
		items = append(items, created...)
	}
	return items, period, nil
}

func (s *AutopilotService) createAutopilotQuotaRejectionNotices(
	ctx context.Context,
	q *db.Queries,
	policy autopilotQuotaPolicy,
	period db.AutopilotQuotaPeriod,
	params db.CreateAutopilotRunParams,
	actorUserID pgtype.UUID,
) ([]db.InboxItem, db.AutopilotQuotaPeriod, error) {
	if policy.action != entitlement.ActionEnforce || policy.notifications == nil ||
		policy.notifications.OnRejection != entitlement.NotificationEveryAttempt {
		return nil, period, nil
	}

	firstAlreadyNotified, err := autopilotQuotaThresholdWasNotified(period, autopilotQuotaFirstRejectionNoticeKey)
	if err != nil {
		return nil, period, err
	}
	audience := autopilotQuotaNoticeAffectedMembers
	if !firstAlreadyNotified {
		audience = autopilotQuotaNoticeAllMembers
	}
	items, err := s.createAutopilotQuotaInboxItems(ctx, q, audience, autopilotQuotaNoticeFacts{
		Kind: "rejected", NoticeKey: autopilotQuotaFirstRejectionNoticeKey,
		WorkspaceID: period.WorkspaceID, AutopilotID: params.AutopilotID, ActorUserID: actorUserID,
		Source: params.Source, Used: period.UsedCount, Reserved: period.ReservedCount,
		Total: period.UsedCount + period.ReservedCount, Limit: policy.limit, ResetAt: policy.resetAt,
	})
	if err != nil {
		return nil, period, err
	}
	if !firstAlreadyNotified && len(items) > 0 {
		period, err = q.MarkAutopilotQuotaThresholdNotified(ctx, db.MarkAutopilotQuotaThresholdNotifiedParams{
			ThresholdKey: autopilotQuotaFirstRejectionNoticeKey,
			WorkspaceID:  period.WorkspaceID,
			PeriodStart:  period.PeriodStart,
			PeriodEnd:    period.PeriodEnd,
		})
		if err != nil {
			return nil, period, fmt.Errorf("mark first autopilot quota rejection notified: %w", err)
		}
	}
	return items, period, nil
}

func autopilotQuotaThresholdWasNotified(period db.AutopilotQuotaPeriod, key string) (bool, error) {
	notified := make(map[string]bool)
	if len(period.NotifiedThresholds) == 0 {
		return false, nil
	}
	if err := json.Unmarshal(period.NotifiedThresholds, &notified); err != nil {
		return false, fmt.Errorf("decode autopilot quota notified thresholds: %w", err)
	}
	return notified[key], nil
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

	details, err := json.Marshal(map[string]string{
		"notice_kind":       facts.Kind,
		"notice_key":        facts.NoticeKey,
		"threshold_percent": strconv.Itoa(facts.ThresholdPercent),
		"used":              strconv.FormatInt(facts.Used, 10),
		"reserved":          strconv.FormatInt(facts.Reserved, 10),
		"total":             strconv.FormatInt(facts.Total, 10),
		"limit":             strconv.FormatInt(facts.Limit, 10),
		"reset_at":          facts.ResetAt.UTC().Format(time.RFC3339),
		"source":            facts.Source,
		"autopilot_id":      util.UUIDToString(facts.AutopilotID),
		"autopilot_title":   autopilotTitle,
	})
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
	if audience == autopilotQuotaNoticeAllMembers {
		return selectedAutopilotQuotaRecipients(members, selected), "", nil
	}

	directActorSelected := false
	if (source == "manual" || source == "api") && actorUserID.Valid {
		if id := util.UUIDToString(actorUserID); memberByID[id].UserID.Valid {
			selected[id] = true
			directActorSelected = true
		}
	}

	autopilot, err := q.GetAutopilot(ctx, autopilotID)
	if err != nil {
		return nil, "", fmt.Errorf("load rejected autopilot for quota notice: %w", err)
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
