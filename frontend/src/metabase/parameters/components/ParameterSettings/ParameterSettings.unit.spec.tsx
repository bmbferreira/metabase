import userEvent from "@testing-library/user-event";

import { renderWithProviders, screen } from "__support__/ui";
import { createMockUiParameter } from "metabase-lib/v1/parameters/mock";
import type { UiParameter } from "metabase-lib/v1/parameters/types";

import { ParameterSettings } from "../ParameterSettings";

interface SetupOpts {
  parameter?: UiParameter;
}

async function fillValue(input: HTMLElement, value: string) {
  await userEvent.clear(input);
  if (value.length) {
    await userEvent.type(input, value);
  }
}

describe("ParameterSidebar", () => {
  it("should allow to change source settings for string parameters", async () => {
    const { onChangeQueryType } = setup({
      parameter: createMockUiParameter({
        type: "string/=",
        sectionId: "string",
      }),
    });

    await userEvent.click(screen.getByRole("radio", { name: "Search box" }));

    expect(onChangeQueryType).toHaveBeenCalledWith("search");
  });

  it("should not update the label if the input is blank", async () => {
    const { onChangeName } = setup({
      parameter: createMockUiParameter({
        name: "foo",
        type: "string/=",
        sectionId: "string",
      }),
    });
    const labelInput = screen.getByLabelText("Label");
    expect(labelInput).toHaveValue("foo");
    await fillValue(labelInput, "");
    // expect there to be an error message with the text "Required"
    expect(screen.getByText(/required/i)).toBeInTheDocument();
    labelInput.blur();
    // when the input blurs, the value should have reverted to the original
    expect(onChangeName).not.toHaveBeenCalled();
    expect(labelInput).toHaveValue("foo");
    // the error message should disappear
    expect(screen.queryByText(/required/i)).not.toBeInTheDocument();

    // sanity check with a non-blank value
    await fillValue(labelInput, "bar");
    labelInput.blur();
    expect(onChangeName).toHaveBeenCalledWith("bar");
    expect(labelInput).toHaveValue("bar");
  });

  it("should not update the label if the input is any variation of the word 'tab'", async () => {
    const { onChangeName } = setup({
      parameter: createMockUiParameter({
        name: "foo",
        type: "string/=",
        sectionId: "string",
      }),
    });
    const labelInput = screen.getByLabelText("Label");
    expect(labelInput).toHaveValue("foo");
    await fillValue(labelInput, "tAb");
    // expect there to be an error message with the text "reserved"
    expect(screen.getByText(/reserved/i)).toBeInTheDocument();
    labelInput.blur();
    // when the input blurs, the value should have reverted to the original
    expect(onChangeName).not.toHaveBeenCalled();
    expect(labelInput).toHaveValue("foo");
    // the error message should disappear
    expect(screen.queryByText(/reserved/i)).not.toBeInTheDocument();

    // sanity check with a non-blank value
    await fillValue(labelInput, "bar");
    labelInput.blur();
    expect(onChangeName).toHaveBeenCalledWith("bar");
    expect(labelInput).toHaveValue("bar");
  });

  it("should allow to change source settings for location parameters", async () => {
    const { onChangeQueryType } = setup({
      parameter: createMockUiParameter({
        type: "string/=",
        sectionId: "location",
      }),
    });

    await userEvent.click(screen.getByRole("radio", { name: "Input box" }));

    expect(onChangeQueryType).toHaveBeenCalledWith("none");
  });

  describe("location", () => {
    beforeEach(() => {
      setup({
        parameter: createMockUiParameter({
          type: "string/=",
          sectionId: "location",
        }),
      });
    });

    it("should render type", () => {
      expect(screen.getByDisplayValue("Location")).toBeInTheDocument();
    });

    it("should render operator", () => {
      expect(screen.getByDisplayValue("Is")).toBeInTheDocument();
    });
  });

  describe("id", () => {
    beforeEach(() => {
      setup({
        parameter: createMockUiParameter({
          type: "id",
          sectionId: "id",
        }),
      });
    });

    it("should render type", () => {
      expect(screen.getByDisplayValue("ID")).toBeInTheDocument();
    });

    it("should not render operator", () => {
      expect(screen.getAllByDisplayValue("id").length).toBe(1);
    });
  });

  describe("string", () => {
    beforeEach(() => {
      setup({
        parameter: createMockUiParameter({
          type: "string/=",
          sectionId: "string",
        }),
      });
    });

    it("should render type", () => {
      expect(screen.getByDisplayValue("Text or Category")).toBeInTheDocument();
    });

    it("should render operator", () => {
      expect(screen.getByDisplayValue("Is")).toBeInTheDocument();
    });
  });

  describe("date", () => {
    beforeEach(() => {
      setup({
        parameter: createMockUiParameter({
          type: "date/single",
          sectionId: "date",
        }),
      });
    });

    it("should render type", () => {
      expect(screen.getByDisplayValue("Time")).toBeInTheDocument();
    });

    it("should render operator", () => {
      expect(screen.getByDisplayValue("Single Date")).toBeInTheDocument();
    });
  });

  describe("number", () => {
    beforeEach(() => {
      setup({
        parameter: createMockUiParameter({
          type: "number/=",
          sectionId: "number",
        }),
      });
    });

    it("should render type", () => {
      expect(screen.getByDisplayValue("Number")).toBeInTheDocument();
    });

    it("should render operator", () => {
      expect(screen.getByDisplayValue("Equal to")).toBeInTheDocument();
    });
  });
});

const setup = ({ parameter = createMockUiParameter() }: SetupOpts = {}) => {
  const onChangeQueryType = jest.fn();
  const onChangeName = jest.fn();

  renderWithProviders(
    <ParameterSettings
      embeddedParameterVisibility={null}
      parameter={parameter}
      isParameterSlugUsed={jest.fn()}
      onChangeName={onChangeName}
      onChangeDefaultValue={jest.fn()}
      onChangeIsMultiSelect={jest.fn()}
      onChangeQueryType={onChangeQueryType}
      onChangeSourceType={jest.fn()}
      onChangeSourceConfig={jest.fn()}
      onChangeRequired={jest.fn()}
    />,
  );

  return { onChangeQueryType, onChangeName };
};
