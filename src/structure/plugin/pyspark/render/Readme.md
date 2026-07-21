# PySpark Render App

## Purpose
Renders PySpark recipes as expressions, schema modules, transform modules, runtime support, projects, and explain
reports.

## Dependency Exchanges
The app consumes opaque PySpark recipes, schemas, and generation settings. It returns source strings and project file
maps through `PySpark.render`; the files app receives those maps through its endpoint contract.

## Inner Workings
Focused render commands cover expressions, steps, schemas, transform modules, runtime modules, projects, and explain
reports. They delegate cross-app schema rendering through `PySpark.schema`.
