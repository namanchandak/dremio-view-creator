import express, { Request, Response } from 'express';
import  scriptRouter  from "./route/script.route";

const app = express();
const port = process.env.PORT || 5555;

app.get('/', (req: Request, res: Response) => {
    res.send('Script server is on port 8080!');
});

app.listen(port, () => {
    console.log(`Script Server running at http://localhost:${port}`);
});

app.use("/" ,scriptRouter )