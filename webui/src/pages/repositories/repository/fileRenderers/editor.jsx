import React from "react";
import Editor from "react-simple-code-editor";
import Prism from "prismjs";
import "prismjs/components/prism-sql";
import "../../../../styles/ghsyntax.css";

export const SQLEditor = ({ value, onChange, onRun }) => {
  return (
    <Editor
      value={value}
      onValueChange={onChange}
      highlight={(code) => Prism.highlight(code, Prism.languages.sql, "sql")}
      padding={10}
      className="syntax-editor"
      onKeyDown={(e) => {
        if (e.key === "Enter" && (e.ctrlKey || e.metaKey)) {
          e.preventDefault();
          e.stopPropagation();
          onRun();
        }
      }}
    />
  );
};
