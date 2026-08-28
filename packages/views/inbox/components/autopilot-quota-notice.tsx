"use client";

import { useCurrentMember } from "@multica/core/permissions";
import { useWorkspacePaths } from "@multica/core/paths";
import type { InboxItem } from "@multica/core/types";
import { Button } from "@multica/ui/components/ui/button";
import { useT } from "../../i18n";
import { useNavigation } from "../../navigation";

function formatResetAt(value: string | undefined): string {
  if (!value) return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "";
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(date);
}

export function AutopilotQuotaNotice({ item }: { item: InboxItem }) {
  const { t } = useT("inbox");
  const { role, isLoading } = useCurrentMember(item.workspace_id);
  const navigation = useNavigation();
  const wsPaths = useWorkspacePaths();
  const details = item.details ?? {};
  const resetAt = formatResetAt(details.reset_at);
  const hasUsageFacts = Boolean(details.total && details.limit && resetAt);

  let body = item.body;
  if (item.type === "autopilot_quota_warning" && hasUsageFacts) {
    body = t(($) => $.detail.autopilot_quota_warning_body, {
      total: details.total,
      limit: details.limit,
      percent: details.threshold_percent,
      resetAt,
    });
  } else if (item.type === "autopilot_quota_exceeded" && details.limit && resetAt) {
    body = details.autopilot_title
      ? t(($) => $.detail.autopilot_quota_exceeded_autopilot_body, {
          autopilot: details.autopilot_title,
          limit: details.limit,
          resetAt,
        })
      : t(($) => $.detail.autopilot_quota_exceeded_body, {
          limit: details.limit,
          resetAt,
        });
  }

  const canManageBilling = role === "owner" || role === "admin";

  return (
    <>
      {body && (
        <div className="mt-4 whitespace-pre-wrap text-body leading-relaxed text-foreground">
          {body}
        </div>
      )}
      {!isLoading &&
        (canManageBilling ? (
          <Button
            className="mt-4"
            size="sm"
            onClick={() => navigation.push(`${wsPaths.settings()}?tab=billing`)}
          >
            {t(($) => $.detail.autopilot_quota_upgrade)}
          </Button>
        ) : (
          <p className="mt-4 text-body text-muted-foreground">
            {t(($) => $.detail.autopilot_quota_contact_admin)}
          </p>
        ))}
    </>
  );
}
