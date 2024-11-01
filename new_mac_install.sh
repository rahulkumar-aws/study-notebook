#Install oh-my-zsh
sh -c "$(curl -fsSL https://raw.github.com/ohmyzsh/ohmyzsh/master/tools/install.sh)"


#Install homebrew
sudo mkdir homebrew && curl -L https://github.com/Homebrew/brew/tarball/master | tar xz --strip 1 -C homebrew
echo 'PATH=$PATH:~/homebrew/sbin:~/homebrew/bin:/opt/local/bin' >> .zshrc
chsh -s /bin/zsh
brew update
export HOMEBREW_NO_ANALYTICS=1
brew analytics off
sudo chown -R $(whoami) /usr/local/lib/pkgconfig

#install brew packages
brew install openvpn
brew install --cask visual-studio-code
brew install --cask notion
brew install --cask powershell
brew install --cask iterm2
brew install --cask google-chrome
brew install --cask microsoft-remote-desktop

#Install Communication Software
brew install --cask discord

#Install zsh tools
brew install zsh-autosuggestions
brew install zsh-syntax-highlighting
brew install zsh-completions
brew install zsh-history-substring-search
brew install zsh-interactive-cd
brew install zsh-navigation-tools


#Download openvpn config
git clone https://github.com/dr0pp3dpack3ts/openssh-files.git
cd openssh-files
./openvpn.sh --import-config --config-file=cyberalvin.ovpn

#Install powerlevel10k
git clone --depth=1 https://github.com/romkatv/powerlevel10k.git ${ZSH_CUSTOM:-$HOME/.oh-my-zsh/custom}/themes/powerlevel10k

zsh_theme="powerlevel10k/powerlevel10k"
sed -i '' "s/robbyrussell/$zsh_theme/g" ~/.zshrc

/usr/bin/script ~/Desktop/iterm2.log


#Starting MACOS services
softwareupdate -i -a --restart
