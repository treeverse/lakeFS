import React from "react";
import { describe, it, expect } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MetadataFields } from "./metadata";

/**
 * MetadataFieldsWrapper is a component wrapper used for testing the MetadataFields component.
 * It uses the actual React useState hook to manage the state passed to MetadataFields,
 * ensuring that state updates automatically trigger re-renders in the test environment.
 *
 * @param {object} props
 * @param {Array<Object>} [props.initialFields=[]] - The initial array of metadata field objects to populate the component's state.
 * @returns {React.JSX.Element} The MetadataFields component rendered with real state management.
 */
const MetadataFieldsWrapper = ({ initialFields }) => {
    const [fields, setFields] = React.useState(initialFields);

    return <MetadataFields metadataFields={fields} setMetadataFields={setFields} />;
};

describe("MetadataFields validation flow", () => {
    it("does not show error when key is valid", () => {
        render(<MetadataFieldsWrapper initialFields={[{ key: "environment", value: "prod", touched: false }]} />);

        expect(screen.queryByText("Key is required")).not.toBeInTheDocument();
    });

    it("shows error when key is empty", () => {
        render(<MetadataFieldsWrapper initialFields={[{ key: "", value: "", touched: true }]} />);

        expect(screen.getByText("Key is required")).toBeInTheDocument();
    });

    it("shows error when key is whitespace only", () => {
        render(<MetadataFieldsWrapper initialFields={[{ key: "  ", value: "", touched: true }]} />);

        expect(screen.getByText("Key is required")).toBeInTheDocument();
    });

    it("shows error after user blurs empty key field", async () => {
        const user = userEvent.setup();

        render(<MetadataFieldsWrapper initialFields={[{ key: "", value: "", touched: false }]} />);
        expect(screen.queryByText("Key is required")).not.toBeInTheDocument();

        const keyInput = screen.getByPlaceholderText("Key");
        await user.click(keyInput);
        await user.tab();

        expect(await screen.findByText("Key is required")).toBeInTheDocument();
    });

    it("clears error when user enters a valid key after blur", async () => {
        const user = userEvent.setup();

        render(<MetadataFieldsWrapper initialFields={[{ key: "", value: "", touched: false }]} />);

        const keyInput = screen.getByPlaceholderText("Key");
        await user.click(keyInput);
        await user.tab();
        expect(await screen.findByText("Key is required")).toBeInTheDocument();

        await user.type(keyInput, "env");

        expect(screen.queryByText("Key is required")).not.toBeInTheDocument();
        expect(keyInput).not.toHaveClass("is-invalid");
        expect(keyInput).toHaveValue("env");
    });

    it("adds a new metadata field row when clicking Add button", async () => {
        const user = userEvent.setup();

        render(<MetadataFieldsWrapper initialFields={[]} />);

        await user.click(screen.getByText(/Add Metadata field/i));

        expect(screen.getAllByPlaceholderText("Key")).toHaveLength(1);
        expect(screen.getByPlaceholderText("Value")).toHaveValue("");
    });

    it("removes the correct metadata row", async () => {
        const user = userEvent.setup();

        render(
            <MetadataFieldsWrapper
                initialFields={[
                    { key: "a", value: "1", touched: false },
                    { key: "b", value: "2", touched: false },
                ]}
            />,
        );

        const firstDeleteButton = screen.getByRole("button", { name: "Remove metadata field 1" });

        await user.click(firstDeleteButton);

        const keyInputs = screen.getAllByPlaceholderText("Key");
        expect(keyInputs).toHaveLength(1);
        expect(keyInputs[0]).toHaveValue("b");
        expect(screen.queryByDisplayValue("a")).not.toBeInTheDocument();
    });
});
