#!/usr/bin/env bash
# defender_audit.sh — robust, jq-free
# Usage:
#   bash defender_audit.sh
#   TENANT_ID=<guid-or-domain> bash defender_audit.sh   # (optional) limit to a tenant

set -euo pipefail

TENANT_ID="${TENANT_ID:-}"

if [[ -n "$TENANT_ID" ]]; then
  echo "🔐 Ensuring login for tenant: $TENANT_ID"
  az login --tenant "$TENANT_ID" --use-device-code >/dev/null
fi

echo "🔍 Fetching subscriptions..."
if [[ -n "$TENANT_ID" ]]; then
  mapfile -t SUBS < <(az account list --all \
    --query "[?tenantId=='${TENANT_ID}' && state=='Enabled'].{id:id,name:name}" -o tsv)
else
  mapfile -t SUBS < <(az account list --all \
    --query "[?state=='Enabled'].{id:id,name:name}" -o tsv)
fi

if [[ ${#SUBS[@]} -eq 0 ]]; then
  echo "⚠️  No enabled subscriptions found for the current context."
  exit 0
fi

# Plans we care about (add/remove as needed)
PLANS=(
  "VirtualMachines"       # Servers (P1/P2 lives here)
  "StorageAccounts"       # Storage
  "SqlServers"            # Azure SQL
  "AppServices"           # App Service
  "KubernetesService"     # AKS
  "KeyVaults"             # Key Vault
  "CosmosDbs"             # Cosmos DB
  "Arm"                   # Resource Manager
  "Dns"                   # DNS
)

for line in "${SUBS[@]}"; do
  sub_id="${line%%$'\t'*}"; sub_name="${line#*$'\t'}"
  echo
  echo "🧭 Subscription: ${sub_name}"
  echo "ID: ${sub_id}"
  echo "----------------------------------------------"
  az account set --subscription "${sub_id}"

  # Show all plans with safe handling for missing subPlan
  printf "%-20s %-10s %-6s\n" "Plan" "Tier" "SubPlan"
  printf "%-20s %-10s %-6s\n" "--------------------" "----------" "------"
  for p in "${PLANS[@]}"; do
    read -r tier subplan < <(az security pricing show -n "$p" \
      --query "[coalesce(pricingTier, 'Free'), coalesce(subPlan, 'None')]" -o tsv 2>/dev/null || echo "Free None")
    printf "%-20s %-10s %-6s\n" "$p" "${tier:-Free}" "${subplan:-None}"
  done

  # Quick opinion on Servers plan
  read -r vm_tier vm_sub < <(az security pricing show -n VirtualMachines \
    --query "[coalesce(pricingTier, 'Free'), coalesce(subPlan, 'None')]" -o tsv 2>/dev/null || echo "Free None")
  if [[ "$vm_tier" == "Standard" && "$vm_sub" == "P2" ]]; then
    echo "❗ Servers: On P2 — if you already have MDE via M365 E5, consider switching to P1 to avoid double-paying."
  elif [[ "$vm_tier" == "Standard" && "$vm_sub" == "P1" ]]; then
    echo "✅ Servers: P1 (predictable fixed pricing)."
  elif [[ "$vm_tier" == "Standard" && "$vm_sub" == "None" ]]; then
    echo "ℹ️ Servers: Standard (legacy). Consider moving to P1/P2 for clarity/predictability."
  else
    echo "❌ Servers: Free/Legacy. If you expect coverage, set to P1 or P2."
  fi

  echo "----------------------------------------------"
done

cat <<'EOF'

💡 To switch servers to Plan 1 (fixed pricing) in a subscription:
az security pricing create -n VirtualMachines --tier Standard --subplan P1 --subscription <SUB_ID>

To set Storage/KeyVault/DNS to Free:
az security pricing create -n StorageAccounts --tier Free --subscription <SUB_ID>
az security pricing create -n KeyVaults       --tier Free --subscription <SUB_ID>
az security pricing create -n Dns             --tier Free --subscription <SUB_ID>
EOF

