# Git version control quickstart

## First-time setup
```bash
git config --global user.name "Your Name"
git config --global user.email "you@example.com"
bash scripts/git_bootstrap.sh
```

## Routine release flow
```bash
python scripts/bump_version.py patch   # or minor|major
bash scripts/release_tag.sh --push
```
