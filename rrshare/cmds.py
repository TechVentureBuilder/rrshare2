# -*- coding: utf-8 -*-

import click


@click.group()
@click.help_option("-h", "--help")
def cli():
    """
    输入 rrshare [COMMAND] --help 查看命令详情
    例如：rrshare update --help
    """
    pass


@cli.command()
def make_config_path():
    """ make dir setting config path 
        .rrsdk /setting/-->  add VALUES to config.json
    """
    from rrshare.RQSetting.rqLocalize import  make_dir_path
    make_dir_path()
    

@cli.command()
def record_data():
    """update all stock and swl data and save to pgsql """
    from rrshare.record_all_data import main_record
    main_record()


@cli.command()
def start_streamlit():
    """start streamlit run PRS.py """
    from rrshare.start_streamlit import main_web
    main_web()


@cli.command()
def realtime_prs():
    """ run Timer.py --> start realtime PRS"""
    from rrshare.Timer import main_timer
    main_timer()


@cli.command()
def ipython():
    """打开 ipython """
    import rrshare
    pass
    try:
        from IPython import embed
    except ImportError:
        click.echo("请安装ipython:pip install ipython")
    else:
        embed()


@cli.command()
def version():
    """获取版本信息"""
    from rrshare import __version__
    click.echo("rrrshare=={}".format(__version__))


if __name__== '__main__':
    cli()

