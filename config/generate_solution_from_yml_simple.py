import os
import sys
import yaml
import json
import subprocess
import time
import shutil
import re
from string import Template
from pathlib import Path
from dotenv import load_dotenv


if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')


def get_fabric_cli_path():
    """Get the path to the Fabric CLI executable."""
    venv_fab = Path(sys.prefix) / 'Scripts' / 'fab.exe'
    if hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix and venv_fab.exists():
        return str(venv_fab)
    return shutil.which('fab') or 'fab'


def run_command(cmd):
    """Execute a subprocess command and return the result."""
    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        encoding='utf-8',
        errors='replace'
    )


def print_command_result(step_name, response):
    """Print subprocess result details."""
    print(f"{step_name} return code: {response.returncode}", flush=True)
    if response.stdout and response.stdout.strip():
        print(f"{step_name} stdout:\n{response.stdout}", flush=True)
    if response.stderr and response.stderr.strip():
        print(f"{step_name} stderr:\n{response.stderr}", flush=True)


def call_azure_api(endpoint, method='get', body=None):
    """Call Azure ARM REST API via Fabric CLI and return status code and response body."""
    cmd = [get_fabric_cli_path(), 'api', endpoint, '-X', method, '-A', 'azure']
    if body:
        cmd.extend(['-i', json.dumps(body)])

    response = run_command(cmd)
    print_command_result(f"Azure API {method.upper()} {endpoint}", response)

    try:
        payload = json.loads(response.stdout) if response.stdout else {}
        return payload.get('status_code', 0), payload.get('text', {})
    except Exception as ex:
        print(f"Failed to parse Azure API response JSON: {ex}", flush=True)
        return 0, {}


def load_config(config_file_path):
    """Load YAML configuration file with environment variable substitution."""
    print(f"Loading config file: {config_file_path}", flush=True)

    with open(config_file_path, encoding='utf-8') as file:
        yaml_content = file.read()

    temp_config = yaml.safe_load(yaml_content)
    yaml_content = yaml_content.replace(
        '{{SOLUTION_VERSION}}',
        temp_config.get('solution_version', 'AV01')
    )

    config = yaml.safe_load(Template(yaml_content).safe_substitute(os.environ))
    print("Config loaded successfully.", flush=True)
    return config


def auth():
    """Authenticate with Azure using service principal credentials."""
    print("Starting Fabric CLI login...", flush=True)

    response = run_command([
        get_fabric_cli_path(),
        'auth', 'login',
        '-u', os.getenv('SPN_CLIENT_ID'),
        '-p', os.getenv('SPN_CLIENT_SECRET'),
        '--tenant', os.getenv('AZURE_TENANT_ID')
    ])

    print("Fabric CLI login completed.", flush=True)
    print(f"Auth return code: {response.returncode}", flush=True)

    # Avoid dumping secrets/tokens; stderr is usually enough for failures
    if response.stderr and response.stderr.strip():
        print(f"Auth stderr:\n{response.stderr}", flush=True)

    if response.returncode != 0:
        raise Exception("Fabric CLI login failed.")


def capacity_exists(capacity_name, subscription_id, resource_group):
    """Check if a Fabric capacity exists in Azure."""
    endpoint = (
        f"/subscriptions/{subscription_id}"
        f"/resourceGroups/{resource_group}"
        f"/providers/Microsoft.Fabric/capacities/{capacity_name}"
        f"?api-version=2023-11-01"
    )
    status, response = call_azure_api(endpoint)
    print(f"Capacity exists check for {capacity_name}: status={status}, response={response}", flush=True)
    return status == 200


def create_capacity(capacity_config, subscription_id, resource_group, defaults):
    """Create or resume a Fabric capacity."""
    capacity_name = capacity_config['name']
    print(f"Processing capacity: {capacity_name}", flush=True)

    if capacity_exists(capacity_name, subscription_id, resource_group):
        print(f"✓ {capacity_name} already exists. Attempting resume...", flush=True)
        resume_endpoint = (
            f"/subscriptions/{subscription_id}"
            f"/resourceGroups/{resource_group}"
            f"/providers/Microsoft.Fabric/capacities/{capacity_name}/resume"
            f"?api-version=2023-11-01"
        )
        status, response = call_azure_api(resume_endpoint, 'post')
        print(f"Resume status for {capacity_name}: {status}", flush=True)
        print(f"Resume response for {capacity_name}: {response}", flush=True)
        return

    admin_members = capacity_config.get('admin_members', defaults.get('capacity_admins', ''))
    if isinstance(admin_members, list):
        admin_members_list = admin_members
    else:
        admin_members_list = [x.strip() for x in admin_members.split(',') if x.strip()]

    request_body = {
        "location": capacity_config.get('region', defaults.get('region')),
        "sku": {
            "name": capacity_config.get('sku', defaults.get('sku')),
            "tier": "Fabric"
        },
        "properties": {
            "administration": {
                "members": admin_members_list
            }
        }
    }

    print(f"Creating capacity {capacity_name} with body: {request_body}", flush=True)

    create_endpoint = (
        f"/subscriptions/{subscription_id}"
        f"/resourceGroups/{resource_group}"
        f"/providers/Microsoft.Fabric/capacities/{capacity_name}"
        f"?api-version=2023-11-01"
    )
    status, response = call_azure_api(create_endpoint, 'put', request_body)

    print(f"Capacity create status for {capacity_name}: {status}", flush=True)
    print(f"Capacity create response for {capacity_name}: {response}", flush=True)

    if status in [200, 201]:
        print(f"✓ Created {capacity_name}", flush=True)
        time.sleep(40)
    else:
        print(f"✗ Failed to create capacity {capacity_name}", flush=True)


def workspace_exists(workspace_name):
    """Check if a Fabric workspace exists."""
    response = run_command([get_fabric_cli_path(), 'ls', f'{workspace_name}.Workspace'])
    print_command_result(f"Check workspace exists {workspace_name}", response)
    return response.returncode == 0


def get_workspace_id(workspace_name):
    """Get the UUID of a workspace by name."""
    response = run_command([
        get_fabric_cli_path(),
        'get',
        f'{workspace_name}.Workspace',
        '-q',
        'id'
    ])
    print_command_result(f"Get workspace id {workspace_name}", response)

    if response.returncode == 0:
        workspace_id = response.stdout.strip()
        if workspace_id:
            return workspace_id

    uuid_match = re.search(
        r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}',
        (response.stdout or '').lower()
    )
    return uuid_match.group() if uuid_match else None


def create_workspace(workspace_config):
    """Create a Fabric workspace and return its ID."""
    workspace_name = workspace_config['name']
    capacity_name = workspace_config['capacity']

    print(f"Processing workspace: {workspace_name} on capacity {capacity_name}", flush=True)

    if workspace_exists(workspace_name):
        print(f"✓ {workspace_name} already exists", flush=True)
        workspace_id = get_workspace_id(workspace_name)
        print(f"Existing workspace ID for {workspace_name}: {workspace_id}", flush=True)
        return workspace_id

    response = run_command([
        get_fabric_cli_path(),
        'create',
        f'{workspace_name}.Workspace',
        '-P',
        f'capacityname={capacity_name}'
    ])
    print_command_result(f"Create workspace {workspace_name}", response)

    if response.returncode != 0:
        raise Exception(f"Failed to create workspace {workspace_name}")

    time.sleep(5)

    workspace_id = get_workspace_id(workspace_name)
    print(f"Workspace ID for {workspace_name}: {workspace_id}", flush=True)

    if not workspace_id:
        raise Exception(f"Workspace {workspace_name} command succeeded but no workspace ID was found.")

    print(f"✓ Created {workspace_name}", flush=True)
    return workspace_id


def assign_permissions(workspace_id, permissions, security_groups):
    """Assign workspace roles to security groups."""
    print(f"Assigning permissions for workspace ID {workspace_id}", flush=True)
    time.sleep(10)

    for permission in permissions:
        group_name = permission.get('group')
        group_id = security_groups.get(group_name)
        role = permission.get('role')

        print(f"Assigning role {role} to group {group_name} ({group_id})", flush=True)

        request_body = {
            "principal": {
                "id": group_id,
                "type": "Group",
                "groupDetails": {
                    "groupType": "SecurityGroup"
                }
            },
            "role": role
        }

        response = run_command([
            get_fabric_cli_path(),
            'api',
            '-X',
            'post',
            f'workspaces/{workspace_id}/roleAssignments',
            '-i',
            json.dumps(request_body)
        ])
        print_command_result(f"Assign permission {role} to {group_name}", response)

        try:
            response_json = json.loads(response.stdout) if response.stdout else {}
        except Exception as ex:
            print(f"Failed to parse permission response JSON: {ex}", flush=True)
            response_json = {}

        status_code = response_json.get('status_code')
        if status_code in [200, 201]:
            print(f"  ✓ Assigned {role} to {group_name}", flush=True)
        else:
            print(f"  ✗ Failed assigning {role} to {group_name}. Response: {response_json}", flush=True)


def get_or_create_git_connection(workspace_id, git_config):
    """Get existing or create new GitHub connection."""
    owner_name = git_config.get('organization')
    repo_name = git_config.get('repository')
    connection_name = f"GitHub-{owner_name}-{repo_name}"

    print(f"Looking for Git connection: {connection_name}", flush=True)

    list_response = run_command([
        get_fabric_cli_path(),
        'api',
        '-X',
        'get',
        'connections'
    ])
    print_command_result("List connections", list_response)

    try:
        list_json = json.loads(list_response.stdout) if list_response.stdout else {}
    except Exception as ex:
        print(f"Failed to parse connections list JSON: {ex}", flush=True)
        list_json = {}

    if list_json.get('status_code') == 200:
        connections = list_json.get('text', {}).get('value', [])
        for conn in connections:
            if conn.get('displayName') == connection_name:
                print(f"✓ Using existing connection: {connection_name}", flush=True)
                return conn.get('id')

    github_url = f"https://github.com/{owner_name}/{repo_name}"
    request_body = {
        "connectivityType": "ShareableCloud",
        "displayName": connection_name,
        "connectionDetails": {
            "type": "GitHubSourceControl",
            "creationMethod": "GitHubSourceControl.Contents",
            "parameters": [
                {
                    "dataType": "Text",
                    "name": "url",
                    "value": github_url
                }
            ]
        },
        "credentialDetails": {
            "credentials": {
                "credentialType": "Key",
                "key": os.getenv('GITHUB_PAT')
            }
        }
    }

    create_response = run_command([
        get_fabric_cli_path(),
        'api',
        '-X',
        'post',
        'connections',
        '-i',
        json.dumps(request_body)
    ])
    print_command_result(f"Create Git connection {connection_name}", create_response)

    try:
        create_json = json.loads(create_response.stdout) if create_response.stdout else {}
    except Exception as ex:
        print(f"Failed to parse create connection JSON: {ex}", flush=True)
        create_json = {}

    if create_json.get('status_code') in [200, 201]:
        connection_id = create_json.get('text', {}).get('id')
        print(f"✓ Created connection: {connection_name}", flush=True)
        return connection_id

    print(f"✗ Failed to create Git connection: {connection_name}. Response: {create_json}", flush=True)
    return None


def connect_workspace_to_git(workspace_id, workspace_name, directory_name, git_config, connection_id):
    """Connect workspace to GitHub repo/folder."""
    print(f"Connecting workspace {workspace_name} to Git folder {directory_name}", flush=True)

    request_body = {
        "gitProviderDetails": {
            "ownerName": git_config.get('organization'),
            "gitProviderType": git_config.get('provider'),
            "repositoryName": git_config.get('repository'),
            "branchName": git_config.get('branch'),
            "directoryName": directory_name
        },
        "myGitCredentials": {
            "source": "ConfiguredConnection",
            "connectionId": connection_id
        }
    }

    response = run_command([
        get_fabric_cli_path(),
        'api',
        '-X',
        'post',
        f'workspaces/{workspace_id}/git/connect',
        '-i',
        json.dumps(request_body)
    ])
    print_command_result(f"Connect workspace {workspace_name} to Git", response)

    try:
        connect_json = json.loads(response.stdout) if response.stdout else {}
    except Exception as ex:
        print(f"Failed to parse Git connect response JSON: {ex}", flush=True)
        connect_json = {}

    if connect_json.get('status_code') in [200, 201]:
        print(f"✓ Connected {workspace_name} to Git: {directory_name}", flush=True)
        return True

    print(f"✗ Failed to connect {workspace_name} to Git. Response: {connect_json}", flush=True)
    return False


def suspend_capacity(capacity_name, subscription_id, resource_group):
    """Suspend a Fabric capacity to stop billing."""
    print(f"Suspending capacity: {capacity_name}", flush=True)

    for attempt in range(1, 6):
        endpoint = (
            f"/subscriptions/{subscription_id}"
            f"/resourceGroups/{resource_group}"
            f"/providers/Microsoft.Fabric/capacities/{capacity_name}/suspend"
            f"?api-version=2023-11-01"
        )
        status, response = call_azure_api(endpoint, 'post')
        print(f"Suspend attempt {attempt} for {capacity_name}: status={status}, response={response}", flush=True)

        if status in [200, 202]:
            print(f"✓ Suspended {capacity_name}", flush=True)
            return True

        time.sleep(60)

    print(f"✗ Failed to suspend {capacity_name}", flush=True)
    return False


def main():
    if not os.getenv('GITHUB_ACTIONS'):
        load_dotenv(Path(__file__).parent.parent / '.env')
        print("Loaded .env for local execution.", flush=True)

    config_file = os.getenv('CONFIG_FILE', 'config/v01/v01-template.yml')
    config = load_config(config_file)

    print("=== AUTHENTICATING ===", flush=True)
    auth()

    azure_config = config['azure']
    subscription_id = azure_config['subscription_id']
    capacity_defaults = azure_config.get('capacity_defaults', {})
    security_groups = azure_config.get('security_groups', {})
    git_config = config.get('github', {})

    print("\n=== CREATING CAPACITIES ===", flush=True)
    for capacity_config in config.get('capacities', []):
        resource_group = capacity_config.get('resource_group', capacity_defaults.get('resource_group'))
        create_capacity(capacity_config, subscription_id, resource_group, capacity_defaults)

    print("\n=== CREATING WORKSPACES ===", flush=True)
    github_connection_id = None

    for workspace_config in config.get('workspaces', []):
        workspace_id = create_workspace(workspace_config)

        if 'permissions' in workspace_config and workspace_id:
            assign_permissions(workspace_id, workspace_config['permissions'], security_groups)

        if 'connect_to_git_folder' in workspace_config and workspace_id and git_config:
            if not github_connection_id:
                github_connection_id = get_or_create_git_connection(workspace_id, git_config)

            if github_connection_id:
                connect_workspace_to_git(
                    workspace_id,
                    workspace_config['name'],
                    workspace_config['connect_to_git_folder'],
                    git_config,
                    github_connection_id
                )

    print("\n=== SUSPENDING CAPACITIES ===", flush=True)
    time.sleep(20)

    for capacity_config in config.get('capacities', []):
        resource_group = capacity_config.get('resource_group', capacity_defaults.get('resource_group'))
        suspend_capacity(capacity_config['name'], subscription_id, resource_group)

    print("\n✓ Done", flush=True)


if __name__ == "__main__":
    main()
