echo "all the steps of the pipeline are going to be run ('itemtoid.py', 'sparql.py', 'wdtotei.py')."
echo "you need to have sourced an python virtual environment with the proper packages installed."
echo "this process can take several hours. do you with to proceed? [y/n]"
read -r input

if [[ ! "$input" =~ (y|n) ]]; then
  echo "invalid answer; you can only answer with y or n" && exit 1
fi;

if [[ "$input" == "n" ]]; then exit 1; fi;

if [[ "$input" == "y" ]]; then
  python main.py -n && python main.py -i && python main.py -s && python main.py -w
fi;
