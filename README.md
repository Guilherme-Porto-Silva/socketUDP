# Servidor de Arquivos via UDP

Sistema cliente-servidor para armazenamento e transferência de arquivos usando **exclusivamente UDP**, com uma camada de transferência confiável construída sobre o protocolo sem conexão. O servidor atende vários clientes simultaneamente por meio de threads, armazena os arquivos em uma pasta central e mantém a lista de arquivos sincronizada em tempo real com todos os clientes ativos. O cliente oferece interface gráfica (Swing) com upload, download, barra de progresso e área de status. Todo o sistema é escrito em **Java puro**, sem dependências externas.

## Requisitos

- **Java Development Kit (JDK)** — versão 8 ou superior.
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
javac -encoding UTF-8 -d out src/comum/*.java src/servidor/*.java src/cliente/*.java
```

### Iniciar o servidor

O servidor deve ser iniciado **antes** do cliente:

```
java -cp out servidor.Servidor <porta>
```

Exemplo — escutando na porta UDP 9876:

```
java -cp out servidor.Servidor 9876
```

### Iniciar o cliente

Em outra janela (ou outra máquina), informe o endereço IP do servidor e a porta:

```
java -cp out cliente.Cliente <ip-do-servidor> <porta>
```

Exemplo — conectando ao servidor `192.168.1.10` na porta 9876:

```
java -cp out cliente.Cliente 192.168.1.10 9876
```

A porta e o endereço são parâmetros de linha de comando e podem ser alterados conforme a rede. A interface gráfica abre após o cliente registrar-se no servidor.

## Arquitetura

### Pacote `src/comum`

Classes compartilhadas entre cliente e servidor.

| Classe | Responsabilidade |
|---|---|
| `Comando.java` | Define os comandos do protocolo — `REGISTER`, `LIST`, `UPLOAD`, `DATA`, `DOWNLOAD`, `UPDATE_LIST` e `ERROR`. |
| `Datagrama.java` | Modela o datagrama trocado entre cliente e servidor, com os campos de controle e o payload do fragmento. |
| `Fragmento.java` | Representa um fragmento do arquivo, contendo o número sequencial e o payload de até 1024 bytes. |

### Pacote `src/servidor`

Lado servidor.

| Classe | Responsabilidade |
|---|---|
| `Servidor.java` | Abre o socket UDP, aguarda datagramas de novos clientes e lança uma thread por cliente. |
| `ClienteHandler.java` | Thread que atende um cliente específico, processando seus comandos e conduzindo a transferência. |
| `GerenciadorArquivos.java` | Gerencia a pasta central de arquivos: salva, renomeia nomes duplicados, lista e localiza arquivos para envio. |
| `LogServidor.java` | Grava o `log_servidor.txt` no formato `[AAAA-MM-DD HH:MM:SS] [IP:porta] [OPERAÇÃO] [STATUS] detalhes`. |

### Pacote `src/cliente`

Lado cliente.

| Classe | Responsabilidade |
|---|---|
| `Cliente.java` | Mantém a comunicação UDP com o servidor e expõe operações de registro, listagem, upload e download. |
| `InterfaceGrafica.java` | Interface Swing com seleção de arquivo, lista sincronizada, botões de upload/download, barra de progresso e área de status. |
| `ThreadTransferencia.java` | Executa upload e download em thread separada da EDT, evitando o travamento da interface. |

## Protocolo

### Comandos

| Comando | Sentido | Campos | Resposta |
|---|---|---|---|
| `REGISTER` | C→S | id cliente | `ACK` |
| `LIST` | C→S | (vazio) | lista arquivos |
| `UPLOAD` | C→S | nome, tamanho, nº de fragmentos, hash | `ACK`/`ERROR` |
| `DATA` | ambos | id transferência, nº do fragmento, payload | `ACK` fragmento |
| `DOWNLOAD` | C→S | nome | metadados + `DATA` |
| `UPDATE_LIST` | S→C | lista atualizada | (opcional) |
| `ERROR` | ambos | código, mensagem | n/a |

### Formato dos datagramas

Os datagramas de controle carregam os campos do comando como texto; apenas o comando `DATA` carrega o payload binário do arquivo. A tabela abaixo relaciona cada campo, seu tamanho e significado:

| Campo | Tamanho | Significado |
|---|---|---|
| `comando` | variável (texto UTF-8) | Identifica a operação: `REGISTER`, `LIST`, `UPLOAD`, `DATA`, `DOWNLOAD`, `UPDATE_LIST` ou `ERROR`. |
| `idCliente` | variável (texto UTF-8) | Identificador único do cliente na sessão. |
| `idTransferencia` | variável (texto UTF-8) | Identificador da transferência em curso; correlaciona os fragmentos ao arquivo enviado. |
| `nome` | variável (texto UTF-8) | Nome do arquivo no servidor. |
| `tamanho` | variável (numérico) | Tamanho total do arquivo em bytes. |
| `numFragmentos` | variável (numérico) | Quantidade total de fragmentos do arquivo. |
| `numFragmento` | variável (numérico) | Índice sequencial do fragmento (de 0 até `numFragmentos - 1`). |
| `hash` | variável (texto UTF-8) | Hash do arquivo, enviado no `UPLOAD` para verificação de integridade. |
| `payload` | até 1024 bytes | Conteúdo do fragmento no comando `DATA`; fragmentos intermediários têm 1024 bytes e o último pode ser menor. |
| `codigo` | variável (numérico) | Código de erro no comando `ERROR`. |
| `mensagem` | variável (texto UTF-8) | Descrição legível do erro no comando `ERROR`. |
| `listaArquivos` | variável (texto UTF-8) | Nomes dos arquivos disponíveis, enviados no `UPDATE_LIST` e na resposta ao `LIST`. |

## Confiabilidade sobre UDP

- **Fragmentação**: arquivos são divididos em pedaços de **1024 bytes (1 KB)**. O tamanho cabe no MTU Ethernet (1500 bytes), o que reduz perda de pacotes e evita fragmentação na camada de rede.
- **Numeração**: cada fragmento é numerado sequencialmente para permitir a remontagem correta no destino.
- **ACK individual**: cada fragmento recebido gera uma confirmação (`ACK`) imediata de volta ao remetente, referenciando o fragmento confirmado.
- **Timeout**: espera de **2 segundos** por confirmação antes de retransmitir.
- **Retransmissão**: limite de **5 tentativas** por fragmento; ao extrapolar, a transferência é abortada e um erro é reportado.
- **Duplicados**: fragmentos duplicados são descartados.
- **Fora de ordem**: fragmentos fora de ordem são armazenados em buffer e remontados quando a transferência estiver completa.
- **Arquivo inexistente**: o servidor responde com `ERROR` ao pedido de download de um nome que não existe.
- **Servidor indisponível**: detectado por timeout no cliente, que exibe mensagem de erro na interface gráfica.

## Decisões de projeto

| Decisão | Justificativa |
|---|---|
| Concorrência com threads (uma por cliente) | Escolha mais simples e segura em Java; evita a complexidade de NIO/select. |
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