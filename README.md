# UK_Job_Vacancy_API
Building an API for job vacancies from public UK ONS data

## Setup

### Environment variables

Copy `.env.example` to `.env` and fill in your database credentials:

```
cp .env.example .env
```

`.env` is listed in `.gitignore` and must never be committed. The required variables are:

| Variable | Description | Default |
|---|---|---|
| `DB_NAME` | PostgreSQL database name | — |
| `DB_USER` | Database user | — |
| `DB_PASSWORD` | Database password | — |
| `DB_HOST` | Database host | `localhost` |
| `DB_PORT` | Database port | `5432` |

When deploying (e.g. Railway), set these as environment variables in the platform rather than using a `.env` file.

## Disclaimer

This project uses data from the UK Office for National Statistics (ONS). The data is licensed under the Open Government Licence v3.0: https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/.

The ONS does not endorse or take responsibility for this project. Any errors in the processing or interpretation of the data are the responsibility of the project maintainer.
