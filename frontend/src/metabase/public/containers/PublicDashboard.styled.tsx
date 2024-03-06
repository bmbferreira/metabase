import styled from "@emotion/styled";
import { color } from "metabase/lib/colors";
import { DashboardTabs } from "metabase/dashboard/components/DashboardTabs";
import {
  breakpointMinLarge,
  breakpointMinSmall,
} from "metabase/styled-components/theme";

export const StyledDashboardTabs = styled(DashboardTabs)`
  border-bottom: 1px solid ${color("border")};

  padding: 0 0.5rem;

  ${breakpointMinSmall} {
    padding: 0 1rem;
  }

  ${breakpointMinLarge} {
    padding: 0 1.5rem;
  }
`;

export const DashboardContainer = styled.div`
  padding-left: 0.5rem;
  padding-right: 0.5rem;

  ${breakpointMinSmall} {
    padding-left: 1rem;
    padding-right: 1rem;
  }

  ${breakpointMinLarge} {
    padding-left: 1.5rem;
    padding-right: 1.5rem;
  }

  padding-top: 0;
`;

export const DashboardGridContainer = styled.div`
  margin-top: 1rem;
`;
