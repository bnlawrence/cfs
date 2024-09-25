# web/management/commands/build_docs.py

from django.core.management.base import BaseCommand
import subprocess

class Command(BaseCommand):
    help = "Build MkDocs documentation"

    def handle(self, *args, **kwargs):
        try:
            subprocess.run(["mkdocs", "build"], check=True)
            self.stdout.write(self.style.SUCCESS("Successfully built the documentation"))
        except subprocess.CalledProcessError as e:
            self.stdout.write(self.style.ERROR(f"Failed to build documentation: {e}"))