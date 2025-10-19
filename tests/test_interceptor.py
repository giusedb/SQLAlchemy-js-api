import pytest
from jsalchemy_web_context import db

@pytest.mark.asyncio
async def test_interceptor_insert(context, filesystem, auth):
    from jsalchemy_api import ResourceManager, DBResource

    Folder, File, Tag = filesystem
    rm = ResourceManager(auth, context)
    DBResource(rm, 'Folder', Folder)
    DBResource(rm, 'File', File)
    DBResource(rm, 'Tag', Tag)
    interceptor = rm.interceptor
    interceptor.connect_m2m()

    async with context():
        interceptor.start_record()
        folder = Folder(name="test")
        file = File(name="test", folder=folder)
        tag = Tag(name="test")
        folder.tags.append(tag)
        file.tags.append(tag)
        db.add_all([folder, file, tag])

        sub_folder = Folder(name='foo', parent=folder)
        db.add(sub_folder)
        await db.flush()
        afile = File(name='foo', folder=sub_folder)
        db.add(afile)

        await db.commit()

        assert folder in interceptor.changes['new']
        assert file in interceptor.changes['new']
        assert tag in interceptor.changes['new']

    async with context():
        interceptor.start_record()
        assert (await db.get(Folder, 1)).name == 'test'
        folder = await db.get(Folder, 1)
        folder.name = 'test2'
        await db.commit()
        assert folder in interceptor.changes['updated']
        assert len(interceptor.new) == 0
        assert len(interceptor.updated) == 1
        assert len(interceptor.deleted) == 0

    async with context():
        interceptor.start_record()
        folder = await db.get(Folder, 1)
        await db.delete(folder)
        await db.commit()
        assert 'Folder' in interceptor.deleted
        assert len(interceptor.new) == 0
        assert len(interceptor.updated) == 0
        assert len(interceptor.deleted) == 1

@pytest.mark.asyncio
async def test_interceptor_update(context, filesystem, auth):
    from jsalchemy_api import ResourceManager, DBResource

    Folder, File, Tag = filesystem
    rm = ResourceManager(auth, context)
    DBResource(rm, 'Folder', Folder)
    DBResource(rm, 'File', File)
    DBResource(rm, 'Tag', Tag)
    interceptor = rm.interceptor
    interceptor.connect_m2m()

    async with context():
        interceptor.start_record()
        folder = Folder(name="test")
        file = File(name="test", folder=folder)
        tag = Tag(name="test")
        folder.tags.append(tag)
        file.tags.append(tag)
        db.add_all([folder, file, tag])

        sub_folder = Folder(name='foo', parent=folder)
        db.add(sub_folder)
        await db.flush()
        afile = File(name='foo', folder=sub_folder)
        db.add(afile)

    async with context():
        interceptor.start_record()
        folder = await db.get(Folder, 1)
        tag = await db.get(Tag, 1)
        file = await db.get(File, 1)
        folder.name = 'test2'
        file.name = 'test2'
        tag.name = 'test2'
        await db.commit()

        assert folder in interceptor.changes['updated']
        assert file in interceptor.changes['updated']
        assert tag in interceptor.changes['updated']

@pytest.mark.asyncio
async def test_interceptor_delete(context, filesystem, auth):
    from jsalchemy_api import ResourceManager, DBResource

    Folder, File, Tag = filesystem
    rm = ResourceManager(auth, context)
    DBResource(rm, 'Folder', Folder)
    DBResource(rm, 'File', File)
    DBResource(rm, 'Tag', Tag)
    interceptor = rm.interceptor
    interceptor.connect_m2m()

    async with context():
        interceptor.start_record()
        folder = Folder(name="test")
        file = File(name="test", folder=folder)
        tag = Tag(name="test")
        folder.tags.append(tag)
        file.tags.append(tag)
        db.add_all([folder, file, tag])

        sub_folder = Folder(name='foo', parent=folder)
        db.add(sub_folder)
        afile = File(name='foo', folder=sub_folder)
        db.add(afile)

    async with context():
        interceptor.start_record()
        folder = await db.get(Folder, 1)
        file = await db.get(File, 1)
        tag = await db.get(Tag, 1)
        await db.delete(folder)
        await db.delete(file)
        await db.delete(tag)
        await db.commit()
        assert 'Folder' in interceptor.deleted
        assert folder.id in interceptor.deleted['Folder']
        assert file.id in interceptor.deleted['File']
        assert tag.id in interceptor.deleted['Tag']

@pytest.mark.asyncio
async def test_interceptor_relations(context, filesystem, auth):
    from jsalchemy_api import ResourceManager, DBResource

    Folder, File, Tag = filesystem
    rm = ResourceManager(auth, context)
    DBResource(rm, 'Folder', Folder)
    DBResource(rm, 'File', File)
    DBResource(rm, 'Tag', Tag)
    interceptor = rm.interceptor
    interceptor.connect_m2m()

    async with context():
        interceptor.start_record()
        folder = Folder(name="test")
        file = File(name="test", folder=folder)
        tag = Tag(name="test")
        db.add_all([folder, file, tag])

    async with context():
        interceptor.start_record()
        folder = await db.get(Folder, 1)
        file = await db.get(File, 1)
        tag = await db.get(Tag, 1)
        (await tag.awaitable_attrs.folders).append(folder)
        (await file.awaitable_attrs.tags).append(tag)
        await db.commit()

    async with context():
        interceptor.start_record()
        folder = await db.get(Folder, 1)
        file = await db.get(File, 1)
        tag = await db.get(Tag, 1)
        await db.flush()
        (await tag.awaitable_attrs.folders).remove(folder)
        await db.commit()

        print(interceptor.changes)