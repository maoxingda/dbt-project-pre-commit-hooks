import json


def check_meta_alias_conf(res):
    res_type = res['resource_type']

    if res_type == 'model':
        assert res.get('config', {}).get('meta', {}).get('alias'), \
            f'🐛🐛🐛{res_type}: ({res["name"]}) missing conf: config.meta.alias'

    elif res_type == 'source':
        assert res.get('meta', {}).get('alias'), \
            f'🐛🐛🐛{res_type}: ({res["name"]}) missing conf: meta.alias'


def check_meta_join_type_conf(res):
    join_types = [
        'inner_join',
        'left_join',
        'right_join',
        'full_join',
    ]

    for column_name, column in res['columns'].items():
        is_foreign_key = False

        if 'foreign_key' in column['meta'].keys():
            is_foreign_key = True
            assert 'join_type' in column['meta'].keys(), \
                f'🐛🐛🐛({res["name"]}.{column_name}) missing conf: join_type'
            assert 'join_condition' in column['meta'].keys(), \
                f'🐛🐛🐛({res["name"]}.{column_name}) missing conf: join_condition'

        elif 'join_type' in column['meta'].keys():
            is_foreign_key = True
            assert 'foreign_key' in column['meta'].keys(), \
                f'🐛🐛🐛({res["name"]}.{column_name}) missing conf: foreign_key'
            assert 'join_condition' in column['meta'].keys(), \
                f'🐛🐛🐛({res["name"]}.{column_name}) missing conf: join_condition'

        elif 'join_condition' in column['meta'].keys():
            is_foreign_key = True
            assert 'foreign_key' in column['meta'].keys(), \
                f'🐛🐛🐛({res["name"]}.{column_name}) missing conf: foreign_key'
            assert 'join_type' in column['meta'].keys(), \
                f'🐛🐛🐛({res["name"]}.{column_name}) missing conf: join_type'

        if is_foreign_key:
            assert column['meta']['join_type'].replace(' ', '_') in join_types, \
                f"🐛🐛🐛({res['name']}.{column_name}) invalid join type: {column['meta']['join_type']}"


def collect_meta_aliases(res, meta_aliases):
    res_type = res['resource_type']

    if res_type == 'model':
        alias = res.get('config', {}).get('meta', {}).get('alias')

        if alias in meta_aliases.keys():
            meta_aliases[alias].append(res['name'])
        else:
            meta_aliases[alias] = [res['name']]

    elif res_type == 'source':
        alias = res.get('meta', {}).get('alias')

        if alias in meta_aliases.keys():
            meta_aliases[alias].append(res['name'])
        else:
            meta_aliases[alias] = [res['name']]


def check_meta_alias_unique(meta_aliases):
    for alias in meta_aliases:
        assert len(meta_aliases[alias]) == 1, \
            f'🐛🐛🐛multi-resources: ({", ".join(meta_aliases[alias])}) have alias: {alias}.'


def main(*args):
    if len(args) > 0 and args[0] == 'gen_docs':
        import subprocess
        subprocess.call('dbt docs generate', shell=True)

    with open('target/manifest.json') as f:
        manifest = json.load(f)

    resources = [
        'nodes',
        'sources',
    ]

    res_meta_aliases = {}

    for resource in resources:
        for res_name, res in manifest[resource].items():
            if 'dwd-api' not in res['tags']:
                continue

            check_meta_alias_conf(res)
            check_meta_join_type_conf(res)

            collect_meta_aliases(res, res_meta_aliases)

    check_meta_alias_unique(res_meta_aliases)


if __name__ == '__main__':
    raise SystemExit(main())
