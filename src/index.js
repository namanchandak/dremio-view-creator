import express from "express";
import process from "process";
// import scriptRouter from "./route/script.route";
import { exec } from "child_process";
import fs from "fs/promises";

const app = express();
const port = process.env.PORT || 5555;

app.get("/", (req, res) => {
  res.send("Script server is on port 8080!");
});

// New route to pass company_id as argument
app.get("/get-data", (req, res) => {
  // Read company_id from the query string
  const companyId = req.query.company_id;
  if (!companyId) {
    return res.status(400).send("Missing company_id query parameter");
  }

  // Pass the company_id as an argument to the python script
  exec(
    `python3 src/custom-field-db-extraction.py ${companyId}`,
    { maxBuffer: 1024 * 500000 },
    (error, stdout, stderr) => {
      if (error) {
        console.error(`Error executing Python script: ${error.message}`);
        return res.status(500).send(`Error: ${error.message}`);
      }
      if (stderr) {
        console.error(`Python stderr: ${stderr}`);
        // Optionally handle stderr if needed
        return res.status(500).send(`Error: ${stderr}`);
      }

      try {
        // Parse the JSON output from the Python script.
        // If you have mixed output, extract the JSON part accordingly.
        const marker = "===JSON_OUTPUT_START===";
        const jsonPart = stdout
          .substring(stdout.indexOf(marker) + marker.length)
          .trim();
        const output = JSON.parse(jsonPart);
        res.json(output);
      } catch (parseError) {
        console.error("Failed to parse JSON output:", parseError);
        res
          .status(500)
          .send("Failed to parse JSON output: " + parseError.message);
      }
    }
  );
});

app.get("/get-extracted-keys", (req, res) => {
  fs.readFile(
    "/home/mohit/Projects/dice/Script-Server/extracted_json_keys.json",
    "utf-8"
  ).then((data) => {
    res.json(JSON.parse(data));
  });
});
app.get("/get-extracted-values", (req, res) => {
  fs.readFile(
    "/home/mohit/Projects/dice/Script-Server/extracted_json_array_data.json",
    "utf-8"
  ).then((data) => {
    res.json(JSON.parse(data));
  });
});
// app.use("/", scriptRouter);

app.listen(port, () => {
  console.log(`Script Server running at http://localhost:${port}`);
});
