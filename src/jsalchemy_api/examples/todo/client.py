from jsalchemy_api.client import ORM
from jsalchemy_api.client.base import Resource
from jsalchemy_api.client.config import from_file, setup_log
from jsalchemy_api.client.rset import RSet

if __name__ == "__main__":
    config = from_file()
    setup_log()
    orm = ORM(config, on={
        'logged-in': lambda status: print('Logged in', status['first_name'], status['last_name']),
        'logged-out': lambda status: print('Logged out', status)
    })
    Folder = orm.get_model('Folder')
    folder: Resource = orm.get('Folder', '55', blocking=True)
    print(folder.name)
    print('folder dict:', folder.RAW)
    print('diff:', folder.DIFF)
    folder.description = 'new'
    print('diff:', folder.DIFF)
    folder.SAVE()
    folder.description = 'old'
    print('diff:', folder.DIFF)
    folder.SAVE()
    print('final diff:', folder.DIFF)
    myFolder = Folder(name='test', description='test')
    myFolder.SAVE()
    new_folder = orm.get('Folder', myFolder.PK)
    print('same:', myFolder is new_folder)
    print('Folder created with id:', myFolder.id)

    folders: RSet = orm.query('Folder', {'name': 'test'})
    print(folders.fetch())

    orm.stop()


