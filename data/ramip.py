from pathlib import Path
import cf
def atomic_cmip(model):
    """
    Create a set of CFA files for each of the simulations in a CMIP organised directory.
    Each CFA     file will include all the atomic datasets within that simulation
    :param model: source_id, should be a directory path
    :type model: string or path instance
    """
    if isinstance(model, str):
        model = Path(model)
    name = model.stem

    def build_view(member):
        """ 
        We build a view of the folder in a member in such a way that we can
        handle multiple variants of the file that might be present
        """
        folder_dict = {}
        for path in Path(member).rglob('*'):
            if path.is_file():
                parts = path.parts[-5:]  # Get the last 5 parts (t/f/g/v/file)
                # Traverse and build the dictionary structure
                current_level = folder_dict
                for p in parts[:-2]:
                    if p not in current_level:
                        current_level[p]={}
                    current_level=current_level[p]
                if parts[-2] not in current_level:
                    current_level[parts[-2]]=[]
                current_level[parts[-2]].append(f'{path},{path.stat().st_size}')
                
        return folder_dict

    mdir = Path(f'{name}_proc')
    mdir.mkdir(exist_ok=True)
    for experiment in model.glob('*'):
        if not experiment.is_dir():
            print(f'skipping non experiment directory {experiment}')
            continue
        for member in experiment.glob('*'):
            if not member.is_dir():
                print(f'skipping non member directory {member}')
                continue
            else:
                folder_dict = build_view(member)
                for table in folder_dict:
                    filelist=[]
                    for field in folder_dict[table]:
                        grid = list(folder_dict[table][field].keys())[0]
                        versions = sorted(folder_dict[table][field][grid].keys())
                        if 'latest' in versions:
                            choice='latest'
                        else:
                            choice = versions[-1]
                        filelist+=folder_dict[table][field][grid][choice]
                        print(f'Added {field} to {table}')
                    outfile = f'{name}_{member.stem}_{table}_input_files.txt'
                    with open(mdir/outfile,'w') as f:
                        f.write('\n'.join(filelist))
if __name__=="__main__":
    import sys
    print(sys.argv)
    atomic_cmip(sys.argv[1])
