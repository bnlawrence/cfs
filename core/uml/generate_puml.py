import tempfile
from pathlib import Path
from core.db.standalone import setup_django

working_dir = tempfile.TemporaryDirectory()
working_path = Path(str(working_dir))
dbfile = str(working_path/'umldb.db')
migrations_location = str(working_path/'migrations')
setup_django(db_file=dbfile, migrations_location=migrations_location)

import codecs
from django.apps import apps

from core.uml.utils import PlantUml


def standard_view(output_file='db-model.pu'):
    """ 
    This is a baked in view that can be run as part of the tests
    """
    models = apps.get_models()
    title_font_size = 14
    generate_with_legend=False
    generate_with_help=False
    generate_with_choices=True
    generate_with_split_choices=True
    omit_history=True
    include=""
    omit=None
    generate_with_omitted_headers=False
    generate_headers_only=False

    generator = PlantUml(
        models,
        title=None,
        title_font_size=title_font_size,
        with_legend=generate_with_legend,
        with_help=generate_with_help,
        with_choices=generate_with_choices,
        split_choices=generate_with_split_choices,
        omit_history=omit_history,
        include=include,
        omit=omit,
        with_omitted_headers=generate_with_omitted_headers,
        generate_headers_only=generate_headers_only,
    )
    uml = generator.generate_puml_class_diagram()

    with codecs.open(output_file, 'w', encoding='utf-8') as file:
        file.write(uml)

if __name__=="__main__":
    here =  Path( __file__ ).absolute() 
    output_file = here.parent.parent.parent/'docs/diagrams/src/db_uml.pu'
    standard_view(output_file)
    working_dir.cleanup()