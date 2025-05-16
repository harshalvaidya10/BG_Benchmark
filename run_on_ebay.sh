#!/bin/bash

# 定义输出的 ZIP 文件名
OUTPUT_ZIP="project_package.zip"

# 定义要打包的文件和目录
# *.sh 会匹配当前目录下所有以 .sh 结尾的文件
FILES_TO_INCLUDE="*.sh"
DIRS_TO_INCLUDE="target workloads conf"

echo "开始创建 ZIP 压缩包: $OUTPUT_ZIP"

# 检查 target 目录是否存在
if [ ! -d "target" ]; then
  echo "警告: 'target' 目录未找到，将跳过。"
  # 从要包含的目录列表中移除 target
  DIRS_TO_INCLUDE="workloads"
fi

# 检查 workloads 目录是否存在
if [ ! -d "workloads" ]; then
  echo "警告: 'workloads' 目录未找到，将跳过。"
  # 如果 target 之前也被跳过了，这里 DIRS_TO_INCLUDE 会变空
  if [ "$DIRS_TO_INCLUDE" == "target workloads" ]; then
      DIRS_TO_INCLUDE="target"
  elif [ "$DIRS_TO_INCLUDE" == "workloads" ]; then
      DIRS_TO_INCLUDE=""
  fi
fi

# 检查是否有 .sh 文件存在
sh_files=(*.sh)
if [ ${#sh_files[@]} -eq 0 ] || [ ! -e "${sh_files[0]}" ]; then
    echo "警告: 当前目录下没有找到 .sh 文件，将跳过。"
    FILES_TO_INCLUDE=""
fi

# 执行 zip 命令
# -r 参数用于递归地包含目录内容
# 将文件列表和目录列表都传递给 zip 命令
# 使用 DIRS_TO_INCLUDE 和 FILES_TO_INCLUDE 变量确保只包含存在的项
# 使用 eval 来正确处理 *.sh 可能为空的情况，或者直接列出（更安全）
# zip -r "$OUTPUT_ZIP" $DIRS_TO_INCLUDE $FILES_TO_INCLUDE (这种方式如果某个变量为空会引入多余空格，但zip通常能处理)

# 更健壮的方式是分别添加，或者构建参数列表
zip_command_args=(-r "$OUTPUT_ZIP")

# 添加存在的目录
if [ -n "$DIRS_TO_INCLUDE" ]; then
  # shellcheck disable=SC2206 # We want word splitting here
  zip_command_args+=($DIRS_TO_INCLUDE)
fi

# 添加存在的 .sh 文件
if [ -n "$FILES_TO_INCLUDE" ]; then
    # Use find for safety with filenames and if no files match
    # find . -maxdepth 1 -name '*.sh' -print0 | xargs -0 -r zip -r "$OUTPUT_ZIP" $DIRS_TO_INCLUDE
    # Or stick to globbing if filenames are simple and rely on shell/zip behavior
    # This adds the literal "*.sh" if no files match in some shells, let's use find for .sh
    found_sh_files=$(find . -maxdepth 1 -name '*.sh' -print)
    if [ -n "$found_sh_files" ]; then
       # shellcheck disable=SC2206 # We want word splitting for find output lines
       zip_command_args+=($found_sh_files)
    fi
fi


# 只有在有东西可添加时才执行 zip
if [ ${#zip_command_args[@]} -gt 2 ]; then # More than just '-r' and archive name
    echo "正在执行: zip ${zip_command_args[*]}"
    zip "${zip_command_args[@]}"

    # 检查 zip 命令的退出状态
    if [ $? -eq 0 ]; then
        echo "压缩包 '$OUTPUT_ZIP' 创建成功。"
    else
        echo "创建压缩包 '$OUTPUT_ZIP' 时出错。"
        exit 1
    fi
else
    echo "没有找到任何要打包的文件或目录，未创建压缩包。"
fi

exit 0