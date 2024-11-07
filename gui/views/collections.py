# views/collections.py
from django.shortcuts import render
from django.http import HttpResponse
from io import BytesIO
import zipfile
from cfs.db.interface import CollectionInterface, TagInterface


def collections(request):
    collections = CollectionInterface.retrieve_all()
    tags = TagInterface.all()
    return render(request, 'gui/collections.html', {'collections': collections, 'tags': tags})

def get_manifests(request, col_id):
    """Return a serialized view of a manifest for downloading."""
    unique_manifests = CollectionInterface.unique_manifests(col_id)
    name = CollectionInterface.retrieve(id=col_id).name
    manifest_fragments = [manifest.fragments_as_text() for manifest in unique_manifests]

    if len(manifest_fragments) == 1:
        file_name = f'{name}_manifest_1.txt'
        response = HttpResponse(manifest_fragments[0], content_type="text/plain")
        response['Content-Disposition'] = f'attachment; filename={file_name}'
    else:
        zip_buffer = BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w') as zip_file:
            for manifest, fragments in zip(unique_manifests, manifest_fragments):
                file_name = f'{name}_manifest_{manifest.id}.txt'
                zip_file.writestr(file_name, fragments)
        response = HttpResponse(zip_buffer.getvalue(), content_type="application/zip")
        response['Content-Disposition'] = f'attachment; filename={name}_manifests.zip'

    return response