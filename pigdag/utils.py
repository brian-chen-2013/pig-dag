import subprocess as sp

def is_file_dir_exists(name):
    cmd = sp.Popen(['hdfs', 'dfs', '-ls', name], stdout=sp.PIPE)
    ret = cmd.wait()
    if not 0 == ret:
        return False
    return True

def is_dir_has_file(dirname, fname):
    cmd = sp.Popen(['hdfs', 'dfs', '-ls', dirname], stdout=sp.PIPE)
    ret = cmd.wait()
    if not 0 == ret:
        return False
    for line in cmd.stdout:
        k = line.strip().split()
        if k[-1] == '%s/%s' %(dirname, fname):
            return True
    return False

def is_dir_has_success(name):
    return is_dir_has_file(name, '_SUCCESS')

def is_dir_has_failure(name):
    return is_dir_has_file(name, '_FAILURE')


if __name__ == "__main__":
    import time
    print is_file_dir_exists('data0')
    print "\n\n\n"
    time.sleep(3)

    print is_dir_has_failure('data0')
    print "\n\n\n"
    time.sleep(3)

    print is_dir_has_success('data0')
    print "\n\n\n"
