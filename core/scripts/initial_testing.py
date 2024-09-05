
from core.db.standalone import setup_django
setup_django()
print('setup done?')
from core.db.models import File

file1 = File(name='myfile1',path='/tmp/myfile1')
file2 = File(name='myfile2',path='/tmp/myfile2')
file1.save()
file2.save()

for file in File.objects.all():
    print(file)
