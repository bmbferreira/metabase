import { css } from "@emotion/react";
import styled from "@emotion/styled";

interface DashboardCardProps {
  isAnimationDisabled?: boolean;
}

export const DashboardCard = styled.div<DashboardCardProps>`
  position: relative;
  z-index: 2;

  /**
  * Dashcards are positioned absolutely so each one forms a new stacking context.
  * The dashcard user is currently interacting with needs to be positioned above other dashcards
  * to make sure it's not covered by absolutely positioned children of neighboring dashcards.
  *
  * @see https://developer.mozilla.org/en-US/docs/Web/CSS/CSS_positioned_layout/Understanding_z-index/Stacking_context
  */
  &:hover {
    z-index: 3;
  }

  .Card {
    position: absolute;
    inset: 0;
    border-radius: 8px;
    box-shadow: 0 1px 3px var(--color-shadow);
  }

  ${props =>
    props.isAnimationDisabled
      ? css`
          transition: none;
        `
      : null};

  @media (prefers-reduced-motion) {
    /* short duration (instead of none) to still trigger transition events */
    transition-duration: 10ms !important;
  }

  /* Google Maps widgets */
  .gm-style-mtc,
  .gm-bundled-control,
  .PinMapUpdateButton,
  .leaflet-container .leaflet-control-container {
    opacity: 0.01;
    transition: opacity 0.3s linear;
  }

  &:hover .gm-style-mtc,
  &:hover .gm-bundled-control,
  &:hover .PinMapUpdateButton,
  .leaflet-container:hover .leaflet-control-container {
    opacity: 1;
  }
`;
