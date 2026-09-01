import { ActivityIndicator, Linking, ScrollView, View } from "react-native";
import { router, useLocalSearchParams } from "expo-router";
import { useQuery } from "@tanstack/react-query";
import { Text } from "@/components/ui/text";
import { Button } from "@/components/ui/button";
import { IconButton } from "@/components/ui/icon-button";
import { inboxListOptions } from "@/data/queries/inbox";
import { memberListOptions } from "@/data/queries/members";
import { useAuthStore } from "@/data/auth-store";
import { useWorkspaceStore } from "@/data/workspace-store";
import { getInboxDisplayTitle } from "@/lib/inbox-display";

export default function InboxNoticeDetail() {
  const { id } = useLocalSearchParams<{ id: string }>();
  const wsId = useWorkspaceStore((s) => s.currentWorkspaceId);
  const wsSlug = useWorkspaceStore((s) => s.currentWorkspaceSlug);
  const userId = useAuthStore((s) => s.user?.id ?? null);
  const { data: items, isLoading } = useQuery(inboxListOptions(wsId));
  const { data: members = [], isLoading: membersLoading } = useQuery(
    memberListOptions(wsId),
  );

  // Read the raw workspace-scoped cache: deduplication can replace a row,
  // but a sheet already opened for a specific notification must remain stable.
  const item = items?.find(
    (candidate) => candidate.id === id && candidate.workspace_id === wsId,
  );
  const role = members.find((member) => member.user_id === userId)?.role;
  const canManageBilling = role === "owner" || role === "admin";
  const webUrl = process.env.EXPO_PUBLIC_WEB_URL?.replace(/\/+$/, "");

  return (
    <View className="flex-1 bg-background">
      <View className="flex-row items-center border-b border-border px-4 py-3">
        <Text className="flex-1 text-lg font-semibold text-foreground">
          {item ? getInboxDisplayTitle(item) : "Notification"}
        </Text>
        <IconButton
          name="close"
          variant="secondary"
          className="size-7 rounded-full"
          onPress={() => router.back()}
          accessibilityLabel="Close notification"
        />
      </View>

      {isLoading ? (
        <View className="flex-1 items-center justify-center">
          <ActivityIndicator />
        </View>
      ) : !item || item.type !== "autopilot_quota_exceeded" ? (
        <View className="px-4 py-8">
          <Text className="text-sm text-muted-foreground text-center">
            This notification is no longer available.
          </Text>
        </View>
      ) : (
        <ScrollView
          className="flex-1"
          contentContainerClassName="gap-5 px-4 py-5"
          showsVerticalScrollIndicator={false}
        >
          {item.body ? (
            <Text className="text-base leading-6 text-foreground">
              {item.body}
            </Text>
          ) : null}

          {!membersLoading && canManageBilling && webUrl && wsSlug ? (
            <Button
              onPress={() =>
                Linking.openURL(`${webUrl}/${wsSlug}/settings?tab=billing`)
              }
              accessibilityLabel="Review billing options"
            >
              <Text>Review billing options</Text>
            </Button>
          ) : !membersLoading ? (
            <Text className="text-sm leading-5 text-muted-foreground">
              {canManageBilling
                ? "Open Multica on the web to review billing options."
                : "Contact a workspace owner or admin to review billing options."}
            </Text>
          ) : null}
        </ScrollView>
      )}
    </View>
  );
}
