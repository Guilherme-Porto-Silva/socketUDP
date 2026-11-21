# Servidor de Arquivos via UDP

Sistema cliente-servidor para armazenamento e transferência de arquivos usando **exclusivamente UDP**, com uma camada de transferência confiável construída sobre o protocolo sem conexão. O servidor atende vários clientes simultaneamente por meio de threads, armazena os arquivos em uma pasta central e mantém a lista de arquivos sincronizada em tempo real com todos os clientes ativos. O cliente oferece interface gráfica (Swing) com upload, download, barra de progresso e área de status. Todo o sistema é escrito em **Java puro**, sem dependências externas.

## Requisitos

- **Java Development Kit (JDK)** — versão 17 ou superior (o código usa `record`; testado com o JDK 21).
- **IntelliJ IDEA** (recomendado) ou apenas o terminal com `javac`/`java`.
- Para o teste distribuído: **duas máquinas** na mesma rede local, com a porta UDP escolhida liberada no firewall.

## Estrutura do projeto

```
servidor-arquivos-udp/
├── src/
│   ├── cliente/
│   │   ├── componentes/
│   │   │   ├── Conexao.java
│   │   │   ├── Direcao.java
│   │   │   ├── EnderecoServidor.java
│   │   │   ├── MetadadosDownload.java
│   │   │   ├── Nivel.java
│   │   │   └── ThreadTransferencia.java
│   │   ├── fantoches/
│   │   │   └── SocketTransferencia.java
│   │   ├── interfaceGrafica/
│   │   │   ├── ArquivoRemoto.java
│   │   │   ├── IconePonto.java
│   │   │   ├── InterfaceGrafica.java
│   │   │   ├── Monitor.java
│   │   │   ├── MonitorDaTransferencia.java
│   │   │   └── RenderizadorArquivo.java
│   │   ├── UDP/
│   │   │   ├── ProtocoloUDP.java
│   │   │   └── RespostaUDP.java
│   │   └── Cliente.java
│   ├── comum/
│   │   ├── Comando.java
│   │   ├── Datagrama.java
│   │   └── Fragmento.java
│   └── servidor/
│       ├── ClienteHandler.java
│       ├── GerenciadorArquivos.java
│       ├── LogServidor.java
│       ├── ProtocoloUDP.java
│       ├── ResultadoRecebimento.java
│       ├── Servidor.java
│       └── Tipo.java
├── .gitignore
└── README.md
```

## Compilação e execução

### Compilar

No diretório raiz do projeto, compile todas as classes para a pasta `out`:

```
javac -encoding UTF-8 -d out src/comum/*.java src/servidor/*.java src/cliente/*.java src/cliente/UDP/*.java src/cliente/componentes/*.java src/cliente/fantoches/*.java src/cliente/interfaceGrafica/*.java
```

No Linux e no macOS, o equivalente é `javac -encoding UTF-8 -d out $(find src -name '*.java')`.

### Iniciar o servidor

O servidor deve ser iniciado **antes** do cliente. A porta é opcional (padrão: 5000). A pasta `central/` e o arquivo `log_servidor.txt` são criados no diretório de onde o servidor é iniciado:

```
java -cp out servidor.Servidor <porta>
```

Exemplo — escutando na porta UDP 9876:

```
java -cp out servidor.Servidor 9876
```

### Iniciar o cliente

Em outra janela (ou outra máquina), informe o endereço IP do servidor e a porta (padrão: `localhost` e 5000):

```
java -cp out cliente.Cliente <ip-do-servidor> <porta>
```

Exemplo — conectando ao servidor `192.168.1.10` na porta 9876:

```
java -cp out cliente.Cliente 192.168.1.10 9876
```

A porta e o endereço são parâmetros de linha de comando e podem ser alterados conforme a rede. A interface gráfica abre mostrando o estado da conexão, e o cliente se registra no servidor em segundo plano.

## Arquitetura

### Pacote `src/comum`

Classes compartilhadas entre cliente e servidor.

| Classe | Responsabilidade |
|---|---|
| `Comando.java` | Define os comandos do protocolo — `REGISTER`, `LIST`, `UPLOAD`, `DATA`, `DOWNLOAD`, `UPDATE_LIST`, `ACK` e `ERROR` —, cada um com um código de 1 byte. |
| `Datagrama.java` | Contrato binário único dos datagramas: serializa e interpreta cada comando, valida os campos e calcula e confere o hash SHA-256. |
| `Fragmento.java` | Representa um fragmento do arquivo, contendo o número sequencial e o payload de até 1024 bytes. |

### Pacote `src/servidor`

Lado servidor.

| Classe | Responsabilidade |
|---|---|
| `Servidor.java` | Único leitor do socket UDP (por meio de `ProtocoloUDP`). Registra clientes (`REGISTER`), entrega cada datagrama à `ClienteHandler` do endereço de origem e cria uma `ClienteHandler` para cada transferência que chega de um socket dedicado do cliente. Gera os ids de transferência e envia `UPDATE_LIST` a todos os clientes registrados. |
| `ClienteHandler.java` | Thread que atende um cliente registrado (`LIST`, `UPLOAD`, `DOWNLOAD`) ou uma única transferência. Recebe os datagramas pela fila que o `Servidor` alimenta; aplica ACK por fragmento, retransmissão, descarte de duplicados, remontagem fora de ordem e conferência do hash. |
| `GerenciadorArquivos.java` | Gerencia a pasta central de arquivos: salva, renomeia nomes duplicados, lista (com tamanho), lê e localiza arquivos para envio. |
| `LogServidor.java` | Grava o `log_servidor.txt` no formato `[AAAA-MM-DD HH:MM:SS] [IP:porta] [OPERAÇÃO] [STATUS] detalhes`. |
| `ProtocoloUDP.java` | Camada de rede do servidor: abre o socket UDP, envia datagramas e recebe com timeout, devolvendo um `ResultadoRecebimento`. |
| `ResultadoRecebimento.java` / `Tipo.java` | Resultado de uma recepção: datagrama válido (`DATAGRAMA`), tempo esgotado (`TIMEOUT`) ou datagrama malformado (`MALFORMADO`). |

### Pacote `src/cliente`

Lado cliente. A classe `Cliente` fica na raiz do pacote; o restante se divide em quatro subpacotes.

| Classe | Responsabilidade |
|---|---|
| `Cliente.java` | Ponto de entrada. Gera o id do cliente, registra-se no servidor (`REGISTER`), escuta `UPDATE_LIST`, solicita a lista inicial (`LIST`) e detecta servidor indisponível por timeout. |
| `UDP/ProtocoloUDP.java`, `UDP/RespostaUDP.java` | Transporte UDP do cliente (socket principal, envio e recepção com timeout) e o resultado de uma recepção (datagrama ou tempo esgotado). |
| `interfaceGrafica/InterfaceGrafica.java` | Interface Swing com seleção de arquivo, lista sincronizada (nome e tamanho), botões de upload/download, barra de progresso com cancelamento e área de status. |
| `interfaceGrafica/Monitor.java`, `MonitorDaTransferencia.java` | Canal pelo qual a transferência informa progresso, status e resultado à interface sem conhecer Swing; a interface lê esses valores por um `Timer` da EDT. |
| `interfaceGrafica/ArquivoRemoto.java`, `RenderizadorArquivo.java`, `IconePonto.java` | Item da lista de arquivos, seu renderizador e o ícone de estado da conexão. |
| `componentes/ThreadTransferencia.java` | Executa upload e download em thread separada da EDT, evitando o travamento da interface. |
| `componentes/Conexao.java`, `Direcao.java`, `EnderecoServidor.java`, `MetadadosDownload.java`, `Nivel.java` | Tipos de apoio: estado da conexão, direção da transferência, endereço do servidor, metadados de um download e gravidade das mensagens de status. |
| `fantoches/SocketTransferencia.java` | Socket UDP dedicado a uma transferência (porta efêmera própria), independente do socket principal do cliente. |

## Protocolo

### Comandos

| Comando | Sentido | Campos | Resposta |
|---|---|---|---|
| `REGISTER` | C→S | id cliente | `ACK` |
| `LIST` | C→S | (vazio) | lista arquivos (datagrama `UPDATE_LIST`) |
| `UPLOAD` | C→S | nome, tamanho, nº de fragmentos, hash | `ACK` com o id da transferência / `ERROR` |
| `DATA` | ambos | id transferência, nº do fragmento, payload | `ACK` fragmento |
| `DOWNLOAD` | C→S | nome | metadados (no layout do `UPLOAD`) + `DATA` |
| `UPDATE_LIST` | S→C | lista atualizada | (opcional) |
| `ACK` | ambos | id transferência, nº do fragmento | n/a |
| `ERROR` | ambos | código, mensagem | n/a |

O `ACK` do `REGISTER` leva id 0 e fragmento 0; o `ACK` do `UPLOAD` devolve o id atribuído pelo servidor; o `ACK` de um `DATA` confirma o fragmento recebido. Ao final de um upload ou download, o servidor envia um `ACK` de conclusão (nº do fragmento `-1`) depois de remontar o arquivo, conferir o hash e gravar.

### Formato dos datagramas

Todo datagrama começa com 2 bytes: a **versão do protocolo** (valor 1) e o **código do comando** (1 a 8). Em seguida vêm os campos do comando, na ordem da tabela abaixo. Números usam big-endian (`int` com 4 bytes, `long` com 8 bytes) e textos usam UTF-8 precedido do comprimento em bytes (`int`). Somente `Datagrama.java` conhece esse formato.

| Comando (código) | Campos, na ordem |
|---|---|
| `REGISTER` (1) | `idCliente` (texto) |
| `LIST` (2) | nenhum |
| `UPLOAD` (3) | `nome` (texto), `tamanhoArquivo` (`long`), `numeroFragmentos` (`int`), `hash` (texto: SHA-256 em hexadecimal, 64 caracteres), `idTransferencia` (`int`: 0 no pedido do cliente; o id da transferência nos metadados de um download) |
| `DATA` (4) | `idTransferencia` (`int`), `numeroFragmento` (`int`), `comprimentoPayload` (`int`), `payload` (1 a 1024 bytes) |
| `DOWNLOAD` (5) | `nome` (texto) |
| `UPDATE_LIST` (6) | `listaArquivos` (texto: um item `nome;tamanho` por linha) |
| `ACK` (7) | `idTransferencia` (`int`), `numeroFragmento` (`int`) |
| `ERROR` (8) | `codigoErro` (`int`), `mensagem` (texto) |

Um datagrama `DATA` com payload completo ocupa 1038 bytes (14 de cabeçalho e 1024 de payload), abaixo do MTU Ethernet de 1500 bytes mesmo com os 28 bytes dos cabeçalhos IPv4 e UDP.

## Confiabilidade sobre UDP

- **Fragmentação**: arquivos são divididos em pedaços de **1024 bytes (1 KB)**. O tamanho cabe no MTU Ethernet (1500 bytes), o que reduz perda de pacotes e evita fragmentação na camada de rede.
- **Numeração**: cada fragmento é numerado sequencialmente para permitir a remontagem correta no destino.
- **ACK individual**: cada fragmento recebido gera uma confirmação (`ACK`) imediata de volta ao remetente, referenciando o fragmento confirmado.
- **Timeout**: espera de **2 segundos** por confirmação antes de retransmitir.
- **Retransmissão**: limite de **5 tentativas** por fragmento; ao extrapolar, a transferência é abortada e um erro é reportado.
- **Duplicados**: fragmentos duplicados são descartados.
- **Fora de ordem**: fragmentos fora de ordem são armazenados em buffer e remontados quando a transferência estiver completa.
- **Integridade**: o hash SHA-256 do arquivo remontado é conferido no servidor (upload) e no cliente (download); hash divergente aborta a operação com erro. O cliente só considera o upload concluído quando recebe o `ACK` de conclusão do servidor.
- **Arquivo inexistente**: o servidor responde com `ERROR` ao pedido de download de um nome que não existe.
- **Servidor indisponível**: detectado por timeout no cliente, que exibe mensagem de erro na interface gráfica.

## Decisões de projeto

| Decisão | Justificativa |
|---|---|
| Concorrência com threads (uma por cliente) | Escolha mais simples e segura em Java; evita a complexidade de NIO/select. O `Servidor` é o único leitor do socket e entrega cada datagrama à fila da thread do endereço de origem, sem disputa pelo socket. |
| Socket dedicado por transferência | A thread de transferência recebe seus ACKs e fragmentos sem disputar datagramas com a escuta de `UPDATE_LIST` no socket principal; o servidor atende cada transferência pelo endereço de origem. |
| Duplicados: renomear automaticamente (sufixo numérico) | Preserva ambos os arquivos; mais seguro do que sobrescrever. |
| Interface gráfica com Swing | Nativo do JDK, sem dependências externas, implementação mais rápida. |
| Fragmento de 1024 bytes (1 KB) | Cabe no MTU Ethernet (1500 bytes), minimiza perdas e fragmentação de rede. |
| ACK individual por fragmento | Mais simples de implementar e depurar do que janela deslizante; menor risco de bugs sutis. |
| Timeout de 2 segundos | Adequado para redes locais. |
| Limite de 5 tentativas | Equilíbrio entre detecção de falha e tempo de resposta. |

## Testes em duas máquinas

1. Confirme que as duas máquinas estão na mesma rede local e que a porta UDP escolhida está liberada no firewall.
2. Compile o projeto nas duas máquinas (ou em uma e copie a pasta `out`).
3. Na máquina **A** (servidor), execute `java -cp out servidor.Servidor 9876`.
4. Na máquina **B** (cliente), execute `java -cp out cliente.Cliente <IP-da-maquina-A> 9876`.
5. Verifique na interface que o cliente foi registrado e que a lista de arquivos aparece vazia (ou com os arquivos já existentes).
6. Faça upload de um arquivo de qualquer extensão e confirme o progresso na barra.
7. Confirme que a máquina **A** (e um segundo cliente ativo, se houver) recebe `UPDATE_LIST` com o novo arquivo, sem reiniciar a interface.
8. Faça download do arquivo na máquina **B** e compare-o com o original (tamanho e conteúdo).
9. Envie novamente o mesmo arquivo para validar a renomeação automática (sufixo numérico).
10. Encerre o servidor e tente uma operação no cliente para confirmar a detecção de indisponibilidade por timeout, com mensagem de erro na GUI.
11. Inspecione o `log_servidor.txt` gerado na máquina **A** para conferir conexões, uploads, downloads, retransmissões, timeouts e erros registrados.

## Log do servidor

O servidor grava o arquivo `log_servidor.txt` no formato:

```
[AAAA-MM-DD HH:MM:SS] [IP:porta] [OPERAÇÃO] [STATUS] detalhes
```

Exemplo ilustrativo:

```
[2026-11-17 23:10:05] [192.168.1.20:54321] [UPLOAD] [SUCESSO] relatorio.pdf (2048 bytes)
```

O log registra conexões, uploads, downloads, retransmissões, timeouts e erros, servindo como evidência da execução real do sistema.

## Artefatos relacionados

- **Relatório técnico acadêmico** — documento separado (3 a 5 páginas) com arquitetura, diagrama de módulos/classes, tabela do protocolo e justificativas das decisões.
- **`log_servidor.txt`** — gerado em execução real pelo servidor.
- O sistema é executável em duas máquinas distintas (servidor + cliente) via rede UDP.