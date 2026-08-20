#!/usr/bin/env bash
# ------------------------------------------------------------------------------
# Script: run_ingest.sh
# Description: Runs one or more ingest job specs, imports emitted documents,
#              optionally runs derived CTC and SUMS jobs for MODEL jobs,
#              and updates metadata when at least one job run succeeds.
# Usage: ./run_ingest.sh job_spec_id1 [job_spec_id2 ...]
# ------------------------------------------------------------------------------

# Operational flow:
# 1) Validate arguments and resolve credentials path.
# 2) Establish environment for docker-based ingest/import/update.
# 3) For each requested job id, run ingest, extract transfer archives,
#    and import JSON/JSON.GZ output via vximporter.
# 4) For MODEL job ids, derive and run associated CTC and SUMS jobs.
# 5) Run metadata updater if one or more job runs succeeded.
# 6) Coordinate metadata updates across hosts with an NFS-safe lock.
# 7) Print one final SUCCESS/FAILED message.

set -uo pipefail

current_tmp_outdir=""
current_tmp_xfer=""
current_job_host=""
updater_lock_held=0
updater_lock_dir=""
updater_lock_owner=""

usage() {
	cat <<'EOF'
Usage:
	./scripts/VXingest_utilities/run_ingest.sh JOB_SPEC_ID_1 [JOB_SPEC_ID_2 ...]

Description:
	Runs one or more ingest job specs. For MODEL job ids, this script also derives
	and runs associated CTC and SUMS job ids. Each job run invokes ingest,
	extracts transfer archives, and imports JSON/JSON.GZ outputs with vximporter.

Required environment:
	CREDENTIALS_FILE   Path to Couchbase credentials file.
										 If unset, defaults to ${HOME}/credentials when HOME is set.

Optional environment:
	WORKING_ROOT_DIR                   Default: /data-ingest/data/working
	PUBLIC_DIR                         Default: /public
	DATA_SOURCE                        Default: unset (no extra raw-data mount)
	CONTAINER_DATA_PATH                Default: DATA_SOURCE when DATA_SOURCE is set
	VXINGEST_IMAGE                     Default: ghcr.io/noaa-gsl/vxingest/ingest:latest
	DOCKER_RUN_USER                    Default: <host uid>:<host gid>
	VXINGEST_DOCKER_USER               Default: DOCKER_RUN_USER
	LOG_LEVEL                          Default: DEBUG
	VXIMPORTER_IMAGE                   Default: ghcr.io/noaa-gsl/vximporter:latest
	VXIMPORTER_DOCKER_USER             Default: DOCKER_RUN_USER
	VXIMPORTER_WORKERS                 Default: 16
	VXIMPORTER_BATCH_SIZE              Default: 1000
	VX_METADATA_UPDATER_IMAGE          Default: ghcr.io/noaa-gsl/vxmetadataupdater:latest
	VX_METADATA_UPDATER_SETTINGS       Default: unset (image defaults used)
	VX_METADATA_UPDATER_DOCKER_USER    Default: uid:gid of CREDENTIALS_FILE
	VX_METADATA_UPDATER_LOCK_STALE_SECONDS
	                                  Default: 7200
	                                  Stale age threshold for recovering updater lock

Examples:
	./scripts/VXingest_utilities/run_ingest.sh JS:METAR:MODEL:RRFS:schedule:job:V01
	./scripts/VXingest_utilities/run_ingest.sh \
		JS:METAR:MODEL:RRFS:schedule:job:V01 \
		JS:METAR:OBS:NETCDF:schedule:job:V01

Concurrency notes:
	- Ingest/import log files include host and pid to reduce collisions.
	- Archived tarballs are prefixed with host name.
	- Metadata updater lock path: WORKING_ROOT_DIR/locks/vxmetadataupdater.lock.d
	  acquired via atomic mkdir (NFS-safe pattern).
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
	usage
	exit 0
fi

# Validates tar archive members to prevent path traversal extraction.
validate_tar_paths() {
	local tar_file="$1"
	local unsafe_entry

	tar -tzf "${tar_file}" >/dev/null
	if unsafe_entry="$(tar -tzf "${tar_file}" | awk '
        /(^\/)|(^\.\.$)|(^\.\.\/)|(\/\.\.\/)|(\/\.\.$)/ { print; found=1; exit }
        END { exit found ? 0 : 1 }
    ')"; then
		echo "Error: refusing to extract ${tar_file}; unsafe archive path: ${unsafe_entry}" >&2
		return 1
	fi
	return 0
}

# Imports one JSON or JSON.GZ file into Couchbase using vximporter.
run_vximporter() {
	local import_file="$1"
	local import_log_file="$2"
	local vximporter_image="${VXIMPORTER_IMAGE:-ghcr.io/noaa-gsl/vximporter:latest}"
	local docker_run_user="${DOCKER_RUN_USER:-$(id -u):$(id -g)}"
	local vximporter_docker_user="${VXIMPORTER_DOCKER_USER:-${docker_run_user}}"
	local vximporter_workers="${VXIMPORTER_WORKERS:-16}"
	local vximporter_batch_size="${VXIMPORTER_BATCH_SIZE:-1000}"
	local container_import_file
	local -a importer_args

	container_import_file="${import_file/#${working_root_dir}/\/opt\/data}"
	if [ "${container_import_file}" = "${import_file}" ]; then
		echo "Error: import file ${import_file} is not under WORKING_ROOT_DIR (${working_root_dir})." >&2
		return 1
	fi

	importer_args=(
		docker run --rm
		--pull=always
		--user "${vximporter_docker_user}"
		--mount "type=bind,source=${working_root_dir},target=/opt/data,readonly"
		--mount "type=bind,source=${CREDENTIALS_FILE},target=/run/config/credentials,readonly"
	)
	if [ -n "${LOG_LEVEL:-}" ]; then
		importer_args+=(--env "LOG_LEVEL=${LOG_LEVEL}")
	fi

	echo "Running vximporter container: ${vximporter_image}"
	echo "Importing ${import_file}; log: ${import_log_file}"
	if [ "${LOG_LEVEL:-}" = "DEBUG" ]; then
		echo "DEBUG: Importer docker invocation:"
		printf '  %q ' "${importer_args[@]}" "${vximporter_image}" \
			-conn /run/config/credentials \
			-file "${container_import_file}" \
			-workers "${vximporter_workers}" \
			-batch-size "${vximporter_batch_size}"
		echo
	fi

	"${importer_args[@]}" "${vximporter_image}" \
		-conn /run/config/credentials \
		-file "${container_import_file}" \
		-workers "${vximporter_workers}" \
		-batch-size "${vximporter_batch_size}" 2>&1 | tee -a "${import_log_file}"
}

# Archives generated tarballs and removes temporary job directories.
cleanup_job_dirs() {
	local tmp_outdir="$1"
	local tmp_xfer="$2"
	local host_tag="$3"

	if [ -n "${tmp_xfer}" ] && [ -d "${tmp_xfer}" ]; then
		mkdir -p "${working_root_dir}/archive"
		while IFS= read -r -d '' tar_file; do
			local archived_name
			archived_name="${host_tag}-$(basename "${tar_file}")"
			echo "Archiving: ${tar_file} to ${working_root_dir}/archive/${archived_name}"
			mv "${tar_file}" "${working_root_dir}/archive/${archived_name}"
		done < <(find "${tmp_xfer}" -type f -name '*.tar.gz' -print0)
	fi

	if [ -n "${tmp_outdir}" ] && [ -d "${tmp_outdir}" ]; then
		rm -rf "${tmp_outdir}"
	fi
	if [ -n "${tmp_xfer}" ] && [ -d "${tmp_xfer}" ]; then
		rm -rf "${tmp_xfer}"
	fi
}

# Trap target: best-effort cleanup for the currently active job temp directories.
cleanup_current_job_dirs() {
	cleanup_job_dirs "${current_tmp_outdir:-}" "${current_tmp_xfer:-}" "${current_job_host:-unknown-host}"
	release_updater_lock
}

trap cleanup_current_job_dirs EXIT INT TERM

lock_dir_mtime_epoch() {
	local lock_dir="$1"
	local mtime
	if mtime="$(stat -c '%Y' "${lock_dir}" 2>/dev/null)"; then
		echo "${mtime}"
		return 0
	fi
	if mtime="$(stat -f '%m' "${lock_dir}" 2>/dev/null)"; then
		echo "${mtime}"
		return 0
	fi
	return 1
}

release_updater_lock() {
	if [[ "${updater_lock_held}" -eq 1 && -n "${updater_lock_dir}" && -d "${updater_lock_dir}" ]]; then
		rm -rf "${updater_lock_dir}" 2>/dev/null || true
	fi
	updater_lock_held=0
	updater_lock_dir=""
	updater_lock_owner=""
}

acquire_updater_lock() {
	local now_epoch
	local started_epoch
	local lock_age
	local stale_seconds
	local lock_info_path

	stale_seconds="${VX_METADATA_UPDATER_LOCK_STALE_SECONDS:-7200}"
	updater_lock_owner="$(hostname):$$"
	updater_lock_dir="${working_root_dir}/locks/vxmetadataupdater.lock.d"
	lock_info_path="${updater_lock_dir}/lock.info"

	mkdir -p "${working_root_dir}/locks" || return 1

	if mkdir "${updater_lock_dir}" 2>/dev/null; then
		updater_lock_held=1
		now_epoch="$(date +%s)"
		{
			echo "owner=${updater_lock_owner}"
			echo "host=$(hostname)"
			echo "pid=$$"
			echo "started_epoch=${now_epoch}"
		} >"${lock_info_path}"
		return 0
	fi

	now_epoch="$(date +%s)"
	started_epoch=""
	if [[ -f "${lock_info_path}" ]]; then
		started_epoch="$(awk -F= '/^started_epoch=/{print $2; exit}' "${lock_info_path}" 2>/dev/null || true)"
	fi
	if [[ -z "${started_epoch}" ]]; then
		started_epoch="$(lock_dir_mtime_epoch "${updater_lock_dir}" 2>/dev/null || true)"
	fi
	if [[ -n "${started_epoch}" ]]; then
		lock_age=$((now_epoch - started_epoch))
		if [[ "${lock_age}" -ge "${stale_seconds}" ]]; then
			echo "Warning: stale updater lock detected (age=${lock_age}s >= ${stale_seconds}s); attempting recovery." >&2
			rm -rf "${updater_lock_dir}" 2>/dev/null || true
			if mkdir "${updater_lock_dir}" 2>/dev/null; then
				updater_lock_held=1
				{
					echo "owner=${updater_lock_owner}"
					echo "host=$(hostname)"
					echo "pid=$$"
					echo "started_epoch=${now_epoch}"
				} >"${lock_info_path}"
				return 0
			fi
		fi
	fi

	echo "Info: metadata updater lock is held by another run; skipping metadata update this run." >&2
	return 1
}

# Runs one ingest/import job end-to-end for a single job id.
# Returns non-zero on failure and increments success_count on success.
run_this_job() {
	local this_job_id="$1"
	local this_job_failed=0
	local hostname
	local pid
	local temp_out_dir
	local temp_xfer_dir
	local log_dir
	local metrics_dir
	local container_out_parent
	local container_xfer_parent
	local container_log_dir
	local container_metrics_dir
	local tmp_outdir
	local tmp_xfer
	local container_tmp_outdir
	local container_tmp_xfer
	local timestamp
	local ingest_log_file
	local import_log_file
	local vxingest_image
	local docker_run_user
	local vxingest_docker_user
	local container_data_path
	local -a ingest_args
	local found_tar_file=false
	local found_import_file=false

	if [[ ! "${this_job_id}" =~ ^JS:.* ]]; then
		echo "Error: job id must start with 'JS:': ${this_job_id}" >&2
		return 1
	fi

	echo "Submitting job with ID: ${this_job_id}"

	pid=$$
	hostname="$(hostname)"
	temp_out_dir="${working_root_dir}/${hostname}/${pid}/temp_outdir"
	temp_xfer_dir="${working_root_dir}/${hostname}/${pid}/temp_xfer"
	log_dir="${working_root_dir}/logs"
	metrics_dir="${working_root_dir}/common/job_metrics"

	container_out_parent="/opt/data/${hostname}/${pid}/temp_outdir"
	container_xfer_parent="/opt/data/${hostname}/${pid}/temp_xfer"
	container_log_dir="/opt/data/logs"
	container_metrics_dir="/opt/data/common/job_metrics"

	mkdir -p "${temp_out_dir}" "${temp_xfer_dir}" "${log_dir}" "${metrics_dir}" || return 1

	tmp_outdir="$(mktemp -d -p "${temp_out_dir}")" || return 1
	current_tmp_outdir="${tmp_outdir}"
	current_job_host="${hostname}"
	tmp_xfer="$(mktemp -d -p "${temp_xfer_dir}")" || {
		cleanup_job_dirs "${tmp_outdir}" "" "${hostname}"
		current_tmp_outdir=""
		current_job_host=""
		return 1
	}
	current_tmp_xfer="${tmp_xfer}"

	container_tmp_outdir="${container_out_parent}/$(basename "${tmp_outdir}")"
	container_tmp_xfer="${container_xfer_parent}/$(basename "${tmp_xfer}")"

	timestamp="$(date +%s)"
	ingest_log_file="${log_dir}/docker-ingest-${hostname}-${pid}-${this_job_id}-${timestamp}.out"
	import_log_file="${log_dir}/docker-import-${hostname}-${pid}-${this_job_id}-${timestamp}.out"
	echo "Ingest log file: ${ingest_log_file}"
	echo "Import log file: ${import_log_file}"

	vxingest_image="${VXINGEST_IMAGE:-ghcr.io/noaa-gsl/vxingest/ingest:latest}"
	docker_run_user="${DOCKER_RUN_USER:-$(id -u):$(id -g)}"
	vxingest_docker_user="${VXINGEST_DOCKER_USER:-${docker_run_user}}"
	ingest_args=(
		docker run --rm
		--pull=always
		--user "${vxingest_docker_user}"
		--mount "type=bind,source=${working_root_dir},target=/opt/data"
		--mount "type=bind,source=${public_dir},target=/public,readonly"
		--mount "type=bind,source=${CREDENTIALS_FILE},target=/run/secrets/CREDENTIALS_FILE,readonly"
	)
	if [ -n "${DATA_SOURCE:-}" ]; then
		container_data_path="${CONTAINER_DATA_PATH:-${DATA_SOURCE}}"
		ingest_args+=(--mount "type=bind,source=${DATA_SOURCE},target=${container_data_path},readonly")
	fi
	if [ -n "${LOG_LEVEL:-}" ]; then
		ingest_args+=(--env "LOG_LEVEL=${LOG_LEVEL}")
	fi

	echo "Running VxIngest container: ${vxingest_image}; log: ${ingest_log_file}"
	if [ "${LOG_LEVEL:-}" = "DEBUG" ]; then
		echo "DEBUG: Ingest docker invocation:"
		printf '  %q ' "${ingest_args[@]}" "${vxingest_image}" \
			-c /run/secrets/CREDENTIALS_FILE \
			-o "${container_tmp_outdir}" \
			-l "${container_log_dir}" \
			-m "${container_metrics_dir}" \
			-x "${container_tmp_xfer}" \
			-j "${this_job_id}"
		echo
	fi
	if ! "${ingest_args[@]}" "${vxingest_image}" \
		-c /run/secrets/CREDENTIALS_FILE \
		-o "${container_tmp_outdir}" \
		-l "${container_log_dir}" \
		-m "${container_metrics_dir}" \
		-x "${container_tmp_xfer}" \
		-j "${this_job_id}" >"${ingest_log_file}" 2>&1; then
		echo "Error: VxIngest run failed for job id: ${this_job_id}" >&2
		this_job_failed=1
	fi

	if [[ "${this_job_failed}" -eq 0 ]]; then
		while IFS= read -r -d '' tar_file; do
			found_tar_file=true
			echo "Extracting: ${tar_file}"
			if ! validate_tar_paths "${tar_file}" || ! tar -xzf "${tar_file}" -C "${tmp_xfer}"; then
				this_job_failed=1
				break
			fi
		done < <(find "${tmp_xfer}" -maxdepth 1 -type f -name '*.tar.gz' -print0)

		if [ "${found_tar_file}" = "true" ]; then
			echo "Finished extracting tar archives."
		fi
	fi

	if [[ "${this_job_failed}" -eq 0 ]]; then
		while IFS= read -r -d '' import_file; do
			found_import_file=true
			if ! run_vximporter "${import_file}" "${import_log_file}"; then
				this_job_failed=1
				break
			fi
		done < <(find "${tmp_xfer}" -type f \( -name '*.json' -o -name '*.json.gz' \) ! -path '*/.*' -print0)

		if [ "${found_import_file}" != "true" ]; then
			echo "No JSON input found in ${tmp_xfer}; skipping import step."
		fi
	fi

	cleanup_job_dirs "${tmp_outdir}" "${tmp_xfer}" "${hostname}"
	current_tmp_outdir=""
	current_tmp_xfer=""
	current_job_host=""

	if [[ "${this_job_failed}" -ne 0 ]]; then
		echo "Error: integrated job run failed for job id: ${this_job_id}" >&2
		return 1
	fi

	success_count=$((success_count + 1))
	return 0
}

# Initializes environment-derived runtime settings used by this script.
establish_environment() {
	working_root_dir="${WORKING_ROOT_DIR:-/data-ingest/data/working}"
	public_dir="${PUBLIC_DIR:-/public}"
	metadata_updater_image="${VX_METADATA_UPDATER_IMAGE:-ghcr.io/noaa-gsl/vxmetadataupdater:latest}"
	metadata_updater_settings="${VX_METADATA_UPDATER_SETTINGS:-}"
	metadata_updater_settings_container="/app/settings.json"
	metadata_updater_docker_user="${VX_METADATA_UPDATER_DOCKER_USER:-}"
	log_level="${LOG_LEVEL:-DEBUG}"
}

# Runs each requested job id, and for MODEL ids runs derived CTC/SUMS jobs.
run_jobs() {
	# Loop through each provided job_id and process jobs sequentially.
	for job_id in "$@"; do
		# Process the requested job id.
		echo "Processing job with job id: $job_id"
		if ! run_this_job "$job_id"; then
			failed=1
			echo "Failed processing job id: $job_id" >&2
			echo "Skipping derived jobs for failed job id: $job_id" >&2
			continue
		fi
		echo "Finished processing job with job id: $job_id"
		# the job id contains ":MODEL:" also process the associated CTC and SUM documents
		if [[ "$job_id" == *":MODEL:"* ]]; then
			model_name="$(echo "$job_id" | cut -d: -f4)"
			# Construct and process the CTC job id
			echo "Processing CTC documents for model: $model_name"
			# CTC ids are like JS:METAR:CTC:RRFSv2_conus_3km_ret_test4_may2024:schedule:job:V01
			ctc_id="JS:METAR:CTC:${model_name}:schedule:job:V01"
			if ! run_this_job "$ctc_id"; then
				failed=1
				echo "Failed processing CTC documents for model: $model_name" >&2
				echo "Skipping SUMS documents for model: $model_name" >&2
				continue
			fi
			echo "Finished processing CTC documents for model: $model_name"
			# Construct and process the SUMS job id
			echo "Processing SUMS documents for model: $model_name"
			# SUMS ids are like JS:METAR:SUMS:RRFSv2_conus_3km_ret_test4_may2024:schedule:job:V01
			sums_id="JS:METAR:SUMS:${model_name}:schedule:job:V01"
			if ! run_this_job "$sums_id"; then
				failed=1
				echo "Failed processing SUMS documents for model: $model_name" >&2
				continue
			fi
			echo "Finished processing SUMS documents for model: $model_name"
		fi
	done
}

# Runs metadata updater container, using optional host-provided settings file.
run_metadata_updater() {
	if ! acquire_updater_lock; then
		return 0
	fi

	echo "update the metadata"
	echo "Running VxMetadataUpdater container: ${metadata_updater_image}"

	local -a metadata_updater_args=(
		docker run --rm
		--pull always
		--user "${metadata_updater_docker_user}"
		--mount "type=bind,source=${working_root_dir},target=/opt/data"
		--mount "type=bind,source=${CREDENTIALS_FILE},target=/run/secrets/CREDENTIALS_FILE,readonly"
		--env "LOG_LEVEL=${log_level}"
	)

	local -a metadata_updater_cmd_args=(
		-c /run/secrets/CREDENTIALS_FILE
	)

	# If settings are provided, mount and pass them; otherwise use image defaults.
	if [[ -n "${metadata_updater_settings}" ]]; then
		metadata_updater_args+=(
			--mount "type=bind,source=${metadata_updater_settings},target=${metadata_updater_settings_container},readonly"
		)
		metadata_updater_cmd_args+=(
			-s "${metadata_updater_settings_container}"
		)
	fi

	if [ "${log_level}" = "DEBUG" ]; then
		echo "DEBUG: VxMetadataUpdater docker invocation:"
		printf '  %q ' "${metadata_updater_args[@]}" "${metadata_updater_image}" "${metadata_updater_cmd_args[@]}"
		echo
	fi

	if ! "${metadata_updater_args[@]}" "${metadata_updater_image}" "${metadata_updater_cmd_args[@]}"; then
		failed=1
		echo "Error: VxMetadataUpdater failed" >&2
		release_updater_lock
		return 1
	fi

	release_updater_lock
	return 0
}

# main processing...
failed=0
can_run_jobs=1
success_count=0

# Ensure at least one job spec id is provided as an argument.
if [[ $# -lt 1 ]]; then
	echo "Usage: $0 JOB_SPEC_ID_1 [JOB_SPEC_ID_2 ...]"
	failed=1
	can_run_jobs=0
fi

# Ensure CREDENTIALS_FILE resolves to an existing file before proceeding.
if [ -n "${CREDENTIALS_FILE:-}" ]; then
	:
elif [ -n "${HOME:-}" ]; then
	CREDENTIALS_FILE="${HOME}/credentials"
else
	echo "Error: CREDENTIALS_FILE is unset and HOME is unset; cannot determine default credentials path." >&2
	failed=1
	can_run_jobs=0
fi

if [[ "${can_run_jobs}" -eq 1 ]]; then
	export CREDENTIALS_FILE
	# A missing credentials file is an error; jobs cannot run without it.
	if [ ! -f "${CREDENTIALS_FILE}" ]; then
		echo "Error: CREDENTIALS_FILE file does not exist: ${CREDENTIALS_FILE}" >&2
		failed=1
		can_run_jobs=0
	fi
fi
# establish the environment variables for the run
if [[ "${can_run_jobs}" -eq 1 ]]; then
	establish_environment
	# Credentials ownership is used for metadata updater docker --user.
	if [[ -z "${metadata_updater_docker_user}" ]]; then
		if ! metadata_updater_docker_user="$(stat -c '%u:%g' "${CREDENTIALS_FILE}" 2>/dev/null || stat -f '%u:%g' "${CREDENTIALS_FILE}")"; then
			failed=1
			can_run_jobs=0
			echo "Error: unable to determine UID:GID for CREDENTIALS_FILE: ${CREDENTIALS_FILE}" >&2
		fi
	fi

	if [[ "${can_run_jobs}" -eq 1 ]]; then
		run_jobs "$@"
		# Update metadata only when at least one job run succeeded.
		if [[ "${success_count}" -gt 0 ]]; then
			run_metadata_updater
		else
			echo "Skipping metadata updater: success_count=${success_count}; requires success_count > 0."
		fi
	fi
fi

if [[ "${failed}" -ne 0 ]]; then
	echo "FAILED - Run completed with one or more failed jobs."
else
	echo "SUCCESS - Run completed successfully with no failed jobs."
fi
