import { t } from "ttag";

import type { Drill } from "metabase/visualizations/types/click-actions";
import type * as Lib from "metabase-lib";

export const combineColumnsDrill: Drill<Lib.CombineColumnsDrillThruInfo> = ({
  clicked,
}) => {
  if (!clicked.column) {
    return [];
  }

  return [
    {
      name: "combine",
      title: t`Combine columns`,
      section: "combine",
      icon: "add",
      buttonType: "horizontal",
    },
  ];
};
