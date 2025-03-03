import express, { Request, Response } from "express";
// import scriptRouter from "./route/script.route";
import { exec } from "child_process";

const app = express();
const port = process.env.PORT || 5555;

app.get("/", (req: Request, res: Response) => {
  res.send("Script server is on port 8080!");
});


// New route to pass company_id as argument
app.get("/get-data", (req: any, res: any) => {
  // Read company_id from the query string
  const companyId = req.query.company_id as string;
  if (!companyId) {
    return res.status(400).send("Missing company_id query parameter");
  }

  // Pass the company_id as an argument to the python script
  exec(`python3 src/create-fire-query.py ${companyId}`, (error, stdout, stderr) => {
    if (error) {
      console.error(`Error executing Python script: ${error.message}`);
      return res
        .status(500)
        .send(`Error executing Python script: ${error.message}`);
    }
    if (stderr) {
      console.error(`Python stderr: ${stderr}`);
      // You might want to still send a success response if stderr is not critical.
    }
    console.log(`Python stdout: ${stdout}`);
    res.send(`Python script executed successfully:\n${stdout}`);
  });
});

// app.use("/", scriptRouter);

app.listen(port, () => {
  console.log(`Script Server running at http://localhost:${port}`);
});
