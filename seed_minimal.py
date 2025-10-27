#!/usr/bin/env python3
"""
Minimal seeding script to create a user, a dummy GWAS file metadata record,
and a project for quick testing of /enrich and /hypothesis flows.

Usage:
  zsh -c 'python3 scripts/seed_minimal.py --email you@example.com --password secret --phenotype T2D --project-name DemoProject'

Environment:
  MONGODB_URI (e.g., mongodb://mongo:27017)
  DB_NAME      (e.g., hypgen)

Outputs:
  Prints created user_id, file_id, and project_id.
"""
import os
import argparse
from db import UserHandler, FileHandler, ProjectHandler


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--email', required=True)
    parser.add_argument('--password', required=True)
    parser.add_argument('--phenotype', required=True)
    parser.add_argument('--project-name', required=True)
    args = parser.parse_args()

    mongodb_uri = os.getenv('MONGODB_URI')
    db_name = os.getenv('DB_NAME')
    if not mongodb_uri or not db_name:
        raise SystemExit('MONGODB_URI and DB_NAME must be set in environment')

    users = UserHandler(mongodb_uri, db_name)
    files = FileHandler(mongodb_uri, db_name)
    projects = ProjectHandler(mongodb_uri, db_name)

    # Create user (idempotent-ish via email uniqueness in handler)
    msg, status = users.create_user(args.email, args.password)
    if status in (200, 201, 400):
        # Either created or already exists
        # Fetch user via verify to get user_id
        verify, vstatus = users.verify_user(args.email, args.password)
        if vstatus != 200:
            raise SystemExit(f'Could not verify user after creation: {verify}')
        user_id = verify['user_id']
    else:
        raise SystemExit(f'Error creating user: {msg}')

    # Create a dummy file metadata entry (no actual file required for enrichment paths)
    file_id = files.create_file_metadata(
        user_id=user_id,
        filename='dummy_gwas.tsv',
        original_filename='dummy_gwas.tsv',
        file_path='data/uploads/{}/dummy_gwas.tsv'.format(user_id),
        file_type='gwas',
        file_size=0,
    )

    # Create a project
    project_id = projects.create_project(
        user_id=user_id,
        name=args.project_name,
        gwas_file_id=file_id,
        phenotype=args.phenotype,
    )

    print({'user_id': user_id, 'file_id': file_id, 'project_id': project_id})


if __name__ == '__main__':
    main()
