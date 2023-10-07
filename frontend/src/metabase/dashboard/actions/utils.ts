import _ from "underscore";

import type { DashboardState } from "metabase-types/store";
import type {
  Dashboard,
  DashboardId,
  DashboardOrderedCard,
  DashboardTabId,
} from "metabase-types/api";

export function getExistingDashCards(
  dashboardState: DashboardState,
  dashId: DashboardId,
  tabId: DashboardTabId,
) {
  const { dashboards, dashcards } = dashboardState;
  const dashboard = dashboards[dashId];
  return dashboard.ordered_cards
    .map(id => dashcards[id])
    .filter(dc => {
      if (dc.isRemoved) {
        return false;
      }
      if (tabId != null) {
        return dc.dashboard_tab_id === tabId;
      }
      return true;
    });
}

export function hasDashboardChanged(
  dashboard: Dashboard,
  dashboardBeforeEditing: Dashboard,
) {
  return !_.isEqual(
    { ...dashboard, ordered_cards: dashboard.ordered_cards.length },
    {
      ...dashboardBeforeEditing,
      ordered_cards: dashboardBeforeEditing.ordered_cards.length,
    },
  );
}

// sometimes the cards objects change order but all the cards themselves are the same
// this should not trigger a save
export function haveDashboardCardsChanged(
  newCards: DashboardOrderedCard[],
  oldCards: DashboardOrderedCard[],
) {
  return (
    !newCards.every(newCard =>
      oldCards.some(oldCard => _.isEqual(oldCard, newCard)),
    ) ||
    !oldCards.every(oldCard =>
      newCards.some(newCard => _.isEqual(oldCard, newCard)),
    )
  );
}
