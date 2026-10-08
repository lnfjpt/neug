# Shared patch-application helper for third_party trees.
#
# include_guard(GLOBAL): safe to include from multiple module scripts;
# the function definitions below stay available everywhere.

include_guard(GLOBAL)

# Apply <patch_file> inside <source_dir>, or detect that it is already
# applied. Uses git apply inside git checkouts (submodules) and the
# patch(1) executable elsewhere. Fails with the captured tool output so
# CI logs are self-diagnosing.
function(_neug_apply_patch source_dir patch_file patch_name)
    # A core.autocrlf checkout on Windows rewrites the .patch files (files
    # in the main repo) to CRLF, and git apply rejects them outright with
    # "corrupt patch at line N": the failure happens while parsing the
    # patch itself, so --ignore-whitespace cannot help. Materialize an
    # LF-normalized copy in the build directory and apply that; on LF-only
    # checkouts the copy is byte-identical to the original.
    file(READ "${patch_file}" _neug_patch_content)
    string(REPLACE "\r\n" "\n" _neug_patch_content "${_neug_patch_content}")
    string(REGEX REPLACE "[^A-Za-z0-9._-]" "_" _neug_patch_stem "${patch_name}")
    set(_normalized_patch
        "${CMAKE_BINARY_DIR}/patch-normalized/${_neug_patch_stem}.patch")
    file(MAKE_DIRECTORY "${CMAKE_BINARY_DIR}/patch-normalized")
    file(WRITE "${_normalized_patch}" "${_neug_patch_content}")
    set(patch_file "${_normalized_patch}")

    execute_process(
        COMMAND git rev-parse --show-toplevel
        WORKING_DIRECTORY "${source_dir}"
        RESULT_VARIABLE _git_check
        OUTPUT_VARIABLE _git_root
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_QUIET)
    file(REAL_PATH "${source_dir}" _source_real_path)
    if(_git_check EQUAL 0)
        file(REAL_PATH "${_git_root}" _git_real_path)
    endif()

    # --ignore-whitespace keeps the patches applicable on Windows when the
    # submodule sources (as opposed to the .patch file) are checked out
    # with CRLF; it is a no-op on LF-only checkouts (Linux/macOS).
    if(_git_check EQUAL 0 AND _git_real_path STREQUAL _source_real_path)
        set(_patch_check_command git apply --ignore-whitespace --check "${patch_file}")
        set(_patch_apply_command git apply --ignore-whitespace "${patch_file}")
        set(_patch_reverse_check_command
            git apply --reverse --ignore-whitespace --check "${patch_file}")
    else()
        find_program(_patch_executable patch REQUIRED)
        set(_patch_check_command
            "${_patch_executable}" -p1 -f --dry-run -i "${patch_file}")
        set(_patch_apply_command
            "${_patch_executable}" -p1 -f -i "${patch_file}")
        set(_patch_reverse_check_command
            "${_patch_executable}" -p1 -f -R --dry-run -i "${patch_file}")
    endif()

    execute_process(
        COMMAND ${_patch_check_command}
        WORKING_DIRECTORY "${source_dir}"
        RESULT_VARIABLE _patch_check
        ERROR_VARIABLE _patch_error)
    if(_patch_check EQUAL 0)
        execute_process(
            COMMAND ${_patch_apply_command}
            WORKING_DIRECTORY "${source_dir}"
            RESULT_VARIABLE _patch_result
            ERROR_VARIABLE _patch_error)
        if(NOT _patch_result EQUAL 0)
            message(FATAL_ERROR
                "Failed to apply ${patch_name}: ${_patch_error}")
        endif()
        message(STATUS "Applied ${patch_name}.")
        return()
    endif()

    execute_process(
        COMMAND ${_patch_reverse_check_command}
        WORKING_DIRECTORY "${source_dir}"
        RESULT_VARIABLE _patch_applied
        ERROR_VARIABLE _patch_reverse_error)
    if(_patch_applied EQUAL 0)
        message(STATUS "${patch_name} is already applied.")
    else()
        message(FATAL_ERROR
            "${patch_name} neither applies nor appears already applied. "
            "Apply error: ${_patch_error} "
            "Reverse-check error: ${_patch_reverse_error}")
    endif()
endfunction()
