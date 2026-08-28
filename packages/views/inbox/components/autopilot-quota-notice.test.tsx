import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { InboxItem, MemberRole } from "@multica/core/types";
import en from "../../locales/en/inbox.json";
import { AutopilotQuotaNotice } from "./autopilot-quota-notice";

const state = vi.hoisted(() => ({
  role: "member" as MemberRole,
  push: vi.fn(),
}));

vi.mock("@multica/core/permissions", () => ({
  useCurrentMember: () => ({
    role: state.role,
    isLoading: false,
  }),
}));

vi.mock("@multica/core/paths", () => ({
  useWorkspacePaths: () => ({ settings: () => "/acme/settings" }),
}));

vi.mock("../../navigation", () => ({
  useNavigation: () => ({ push: state.push }),
}));

vi.mock("../../i18n", () => ({
  useT: () => ({
    t: (accessor: (dict: unknown) => string, params?: Record<string, string>) => {
      const template = accessor(en);
      if (!params) return template;
      return template.replace(
        /\{\{(\w+)\}\}/g,
        (_, key: string) => params[key] ?? "",
      );
    },
  }),
}));

function item(overrides: Partial<InboxItem> = {}): InboxItem {
  return {
    id: "inbox-1",
    workspace_id: "workspace-1",
    recipient_type: "member",
    recipient_id: "member-1",
    actor_type: "system",
    actor_id: null,
    type: "autopilot_quota_warning",
    severity: "attention",
    issue_id: null,
    title: "Autopilot usage reached 80%",
    body: "Fallback body",
    issue_status: null,
    read: false,
    archived: false,
    created_at: "2026-08-28T08:00:00Z",
    details: {
      threshold_percent: "80",
      total: "80",
      limit: "100",
      reset_at: "2026-09-01T00:00:00Z",
    },
    ...overrides,
  };
}

describe("AutopilotQuotaNotice", () => {
  it("shows managers the localized usage facts and billing action", () => {
    state.role = "owner";
    state.push.mockClear();

    render(<AutopilotQuotaNotice item={item()} />);

    expect(screen.getByText(/80 of 100 autopilot runs/)).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "View upgrade options" }));
    expect(state.push).toHaveBeenCalledWith("/acme/settings?tab=billing");
  });

  it("asks regular members to contact a workspace manager", () => {
    state.role = "member";

    render(
      <AutopilotQuotaNotice
        item={item({
          type: "autopilot_quota_exceeded",
          details: {
            limit: "100",
            reset_at: "2026-09-01T00:00:00Z",
            autopilot_title: "Daily triage",
          },
        })}
      />,
    );

    expect(screen.getByText(/Daily triage/)).toBeInTheDocument();
    expect(
      screen.getByText("Contact a workspace owner or admin to upgrade the plan."),
    ).toBeInTheDocument();
    expect(screen.queryByRole("button")).toBeNull();
  });
});
