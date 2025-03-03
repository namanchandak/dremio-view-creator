import express, { Request, Response } from 'express';

const app = express();
const port = process.env.PORT || 8080;

app.get('/', (req: Request, res: Response) => {
    res.send('Script server is on port 8080!');
});

app.listen(port, () => {
    console.log(`Script Server running at http://localhost:${port}`);
});